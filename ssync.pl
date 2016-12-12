#!/usr/bin/perl -w
use threads;
use threads::shared;
use Thread::Semaphore;
use POSIX;
use strict;
use Path::Class;
use DBI;
use Switch;
use Parse::Pidl;
use File::Copy;
use File::Basename;
use Net::Ping;

my $version="3.6.3";
my $config_file = 'ssync.conf';
my $lockfile = 'ssync.lock';
my $lockupfile = '/somfolder/ssync.lock'; #add folder, where need lock file for lock update, while sync in progress, or update folder in progress!!!

my $db;
my $dbs;
my $st;
my $st_rem;
my $sts;
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) ="";
my $temp=`cat $config_file | grep 'home=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$temp =~ s/([\r\n])//g;
my $home_dir=dir("$temp");
$temp=`cat $config_file | grep 'log=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$temp =~ s/([\r\n])//g;
my $end_dir=dir("$temp");

my $stop_file="$home_dir".'/stop/my_sync';
my $sync_list="$home_dir".'/sync_list';

$temp=`cat $config_file | grep 'destination=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$temp =~ s/([\r\n])//g;
my $folder_dest = dir("$temp");

$temp=`cat $config_file | grep 'origin=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$temp =~ s/([\r\n])//g;
my $folder_origin = dir("$temp");

$temp=`cat $config_file | grep 'smb_folder=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$temp =~ s/([\r\n])//g;
my $folder_point = dir("$temp");

my $errlog_end="$end_dir".'/my_sync.err';
my $log_end="$end_dir".'/my_sync.log';
my $fileslog_end="$end_dir".'/my_sync_files.log';
my $failslog_end="$end_dir".'/my_sync_fails';

my $errlog=$folder_dest->file("my_sync.err");
open (my $errlog_file, '>>', "$errlog") or print "Open $errlog failed\n";
my $log=$folder_dest->file("my_sync.log");
open (my $log_file, '>>', "$log") or print $errlog_file "Open $log failed\n";
my $fileslog=$folder_dest->file("my_sync_files.log");
open (my $fileslog_file, '>>', "$fileslog") or print $errlog_file "Open  $fileslog failed\n";
my $failslog=$folder_dest->file("my_sync_fails");
open (my $failslog_file, '>>', "$failslog") or print $errlog_file "Open  $failslog failed\n";

date_up();
print "$year/$mon/$mday $hour:$min:$sec : my_sync already runing!\n" if -e $lockfile;
print $errlog_file "$year/$mon/$mday $hour:$min:$sec : my_sync already runing!\n" if -e $lockfile;
exit if -e $lockfile;
print "$year/$mon/$mday $hour:$min:$sec : update already runing need wait!\n" if -e $lockupfile;
print $errlog_file "$year/$mon/$mday $hour:$min:$sec : update already runing need wait!\n" if -e $lockupfile;
exit if -e $lockupfile;
`touch $lockfile`;
`touch $lockupfile`;

my $db_path="$folder_dest".'files.db';
my $dbs_path="$folder_dest".'point.db';

my @num;

my $user=`cat $config_file | grep 'login=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$user =~ s/([\r\n])//g;
my $pass=`cat $config_file | grep 'pass=' | awk 'BEGIN { FS = "=" } ; {print \$2}'`;
$pass =~ s/([\r\n])//g;
my $var1=0;

#my $includes=`cat $config_file | grep 'include=' | grep -v '#'| awk 'BEGIN { FS = "=" } ; {print \$2}'`;
#my $excludes=`cat $config_file | grep 'exclude=' | grep -v '#'| awk 'BEGIN { FS = "=" } ; {print \$2}'`;

if (! -e $db_path)
{
    my $dir = dirname("$db_path");
    mkdir $dir;
    open (my $dbf, '>', $db_path);
    close $dbf;
}
if (! -e $dbs_path)
{
    my $dir = dirname("$dbs_path");
    mkdir $dir;
    open (my $dbsf, '>', $dbs_path);
    close $dbsf;
}

if ($ARGV[0])
{
    switch ($ARGV[0]){
        case ["--help","--usage"]{
            print_help();
            unlink $lockfile;
            unlink $lockupfile;
            exit;
        }
        case "--create"{
            create_db();
            unlink $lockfile;
            unlink $lockupfile;
            exit;
          }
        case "--update"{
            update_db();
            unlink $lockfile;
            unlink $lockupfile;
            exit;
          }
        case "--compare"{
            if (! $ARGV[1])
            { print "Need IP!!!!!"; unlink $lockfile; exit;}
            @num = $ARGV[1];
            create_point_db();
            compare_with_point_db();
            show_files();
            send_files();
            unlink $lockfile;
            unlink $lockupfile;
            exit;
          }
        case "--version"{
            print "my_sync2, version: $version.\n";
            unlink $lockfile;
            unlink $lockupfile;
            exit;
          }
        else {
            print_help();
            unlink $lockfile;
            unlink $lockupfile;
            exit;
        }
    }
}

date_up();
print $log_file "\n$year/$mon/$mday $hour:$min:$sec : Synchronization $version started\n";
print "$year/$mon/$mday $hour:$min:$sec : Synchronization $version started\n";

#-----------------------------------Проверка дисков сетевых---------------------
my $diru = dir("/windows/disk/");
my $fileu = $diru->file("u_o_" . "my_sync.test");
open (my $fhu, '>', $fileu);
print $fhu ':)';
close $fhu;
if ( -s $fileu )
{
  unlink $fileu;
} else
{
  date_up();
  print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Disks not found!!!\n";
  unlink $lockfile;
  unlink $lockupfile;
  return;
}
#-------------------------------------------------------------------------------
my @threads;
my @num_norm;
my $s = new Thread::Semaphore;
my $glob_result :shared = 0;
#Uncomment for tests:
#@num_norm='192.168.1.2';
chdir "$folder_origin";

my $i=0;

update_db();
my @files_to_send;
show_files();
my $size = @files_to_send;
if ( $size == 0 )
{
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Nothing to do...\n\n";
    print "$year/$mon/$mday $hour:$min:$sec : Nothing to do...\n";
    `cp -u "$log" "$log_end"`;
    unlink $lockfile;
    unlink $lockupfile;
    exit 0;
}else{
    my $count_num=`cat $sync_list | wc -l`;

    @num=`cat $sync_list`;
    date_up();
    print "$year/$mon/$mday $hour:$min:$sec : IP's for sync:\n";
    foreach my $num (@num)
    {
      $num =~ s/([\r\n])//g;
      print "$year/$mon/$mday $hour:$min:$sec : $num\n";
    }

    my $p = Net::Ping->new("icmp");
    foreach my $num (@num)
    {
        date_up();
        print $log_file "$year/$mon/$mday $hour:$min:$sec : Host $num - ";
        print "$year/$mon/$mday $hour:$min:$sec : Host $num - ";
        if ( $p->ping($num, 2) or $p->ping($num, 2) )
        {
            push @num_norm, $num;
            print $log_file "OK\n";
            print "OK\n";
        }else{
          date_up();
          print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Sadly, no ping to IP $num...\n";
          print $log_file "fail\n$year/$mon/$mday $hour:$min:$sec : Sadly, no ping to IP $num...\n";
          print "fail\n";
          next;
        }
    }
    $p->close();

    #create links for sending files.--------------------------------------------
    link_cr();
    #end creating --------------------------------------------------------------

    foreach my $num (@num_norm){
        push @threads, threads->create(\&send_files, "$num")->join();
        date_up();
        print "$year/$mon/$mday $hour:$min:$sec : Thread for $num created.\n";
        sleep 1;
    }
    #foreach my $thread (@threads) {
    #    $thread->join();
    #}

    #delete links for sending files.--------------------------------------------
    link_del();
    #end delete-----------------------------------------------------------------
    if ($glob_result eq 0){
        $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
        $st=$db->prepare("select * from files where mark=1");
        $st->execute();
        while(my @row = $st->fetchrow_array()) {
        #$st_rem->execute($row[0]) or print $errlog_file "update failed(((";
            my ($device, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat("$folder_origin/$row[0]");
            $st_rem=$db->prepare("update files set date = ?, mark = 0 where fullpath=?");
            $st_rem->execute($mtime,$row[0]);
            #$st_rem=$db->prepare("update files set mark = 0 where fullpath=?");
            #$st_rem->execute($row[0]);
            $st_rem->finish();
      }

    }else{
        date_up();
        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : Files not PUSHED to any hosts retry it later...\n";
        print "$year/$mon/$mday $hour:$min:$sec : Files not PUSHED to any hosts retry it later...\n";
    }
    #send_files();  #-----------------------------------------------------------!!!!!!!!!!!!!!
    if ( -e $failslog ){
        while ( `cat $failslog` ){
              @num_norm = `cat $failslog`;
              unlink $failslog;
              link_cr();
              foreach my $num (@num_norm){
                  push @threads, threads->create(\&send_files, "$num")->join();
                  date_up();
                  print "$year/$mon/$mday $hour:$min:$sec : Thread for $num created.";
                  sleep 1;
              }

              link_del();
              sleep 60;
        }
    }

}

date_up();
print $log_file "$year/$mon/$mday $hour:$min:$sec : Synchronization $version complete\n\n";
print "$year/$mon/$mday $hour:$min:$sec : Synchronization $version complete\n";
`cp "$errlog" "$errlog_end"`;
`cp "$log" "$log_end"`;
`cp "$fileslog" "$fileslog_end"`;
#---------------------------------------------------------------------SUBS
sub send_files {
    my $num = shift;
    $num =~ s/([\r\n])//g;
    my $result = 0;
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Send to $num started...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Send to $num started...\n";

    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $st=$db->prepare("select * from files where mark=1");
    $st->execute();
    while(my @row = $st->fetchrow_array()) {
        date_up();
        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : Try push file $row[0] to $num...\n";
        print "$year/$mon/$mday $hour:$min:$sec : Try push file $row[0] to $num...\n";
        $result = 0;
        my $dir = "$row[2]";
        $dir =~ s/\//\\/g;
        #`ln -s "$folder_origin/$row[2]/$row[1]" "$folder_dest/$row[2]/$row[1]_new"`;
        chdir "$folder_dest/$row[2]";

        `smbclient //$num/base -U $user $pass -c "cd $folder_point\\$row[2]; prompt; mput "$row[1]_new"; exit"`;
        my $row_temp=$row[1];
        $row_temp =~ s/\[/\\[/g;
        $row_temp =~ s/\]/\\]/g;
        my $temp_size_destination = `smbclient //$num/base -U $user $pass -c "cd $folder_point\\$row[2]; ls; exit;" | grep "$row_temp"_new | awk '{print \$3}'`;
        $temp_size_destination =~ s/([\r\n])//g;
        my $temp_size_office = `ls -la $folder_origin/$row[2]/$row[1] | awk '{print \$5}'`;
        $temp_size_office =~ s/([\r\n])//g;
        #print $log_file "$num, $row[2]/$row[1]: $temp_size_destination : $temp_size_office\n";
        if ( $temp_size_destination eq $temp_size_office) {
        } else {
            $result = 1;
            date_up();
            print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Sendinging file $row[0] has been failed for $num $temp_size_destination <> $temp_size_office.\n";
            print $errlog_file "$year/$mon/$mday $hour:$min:$sec : File will be send again later.\n";
            print "$year/$mon/$mday $hour:$min:$sec : Sending file $row[0] has been failed for $num $temp_size_destination <> $temp_size_office.\n";
            print "$year/$mon/$mday $hour:$min:$sec : File will be send again later.\n";
            print $failslog_file "$num\n";
            #`rm -f "$folder_dest/$row[2]/$row[1]_new"`;
            next;
        }
        `smbclient //$num/base -U $user $pass -c "cd $folder_point\\$row[2]; prompt; del "$row[1]"; exit"`;
        my $ls=`smbclient //$num/base -U $user $pass -c "cd $folder_point\\$row[2]; ls "$row[1]"; exit" | wc -l`;
        if ($ls eq 3){
            $result=1;
            date_up();
            print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Deleting file $row[0] has been failed for $num.\n";
            print $errlog_file "$year/$mon/$mday $hour:$min:$sec : File will be send again later.\n";
            print "$year/$mon/$mday $hour:$min:$sec : Deleting file $row[0] has been failed for $num.\n";
            print "$year/$mon/$mday $hour:$min:$sec : File will be send again later.\n";
            print $failslog_file "$num\n";
            #`rm -f "$folder_dest/$row[2]/$row[1]_new"`;
            next;
        } else {
            `smbclient //$num/base -U $user $pass -c "cd $folder_point\\$row[2]; prompt; rename "$row[1]_new" "$row[1]"; exit"`;
        }
        date_up();
        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : File $row[0] PUSHED to $num...\n";
        print "$year/$mon/$mday $hour:$min:$sec : File $row[0] PUSHED to $num...\n";
        #`rm -f "$folder_dest/$row[2]/$row[1]_new"`;
        chdir "$folder_origin";
    }
    date_up();
    print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : Files sended to $num...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Files sended to $num...\n";
    if ("$result" eq "0"){
    }else{
        date_up();
        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : Files not PUSHED to any hosts retry it later...\n";
        print "$year/$mon/$mday $hour:$min:$sec : Files not PUSHED to any hosts retry it later...\n";
        $s->down;
        if ("$glob_result" eq "0"){
          $glob_result = 1;
        }
        $s->up;
    }
    #$st_rem->finish();
    $st->finish();
    $db->disconnect();
}
#------------------------------------------------
sub date_up {
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
  $mon=$mon+1;
  $year=$year+1900;
  if (length($mon)  == 1) {$mon = "0$mon";}
  if (length($mday) == 1) {$mday = "0$mday";}
  if (length($hour) == 1) {$hour = "0$hour";}
  if (length($min) == 1) {$min = "0$min";}
  if (length($sec) == 1) {$sec = "0$sec";}
}
#------------------------------------------------
sub show_files {
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Next files must be synced:\n\n";
    chdir "$folder_origin";
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $st=$db->prepare("select * from files where mark=1");
    $st->execute();
    while(my @row = $st->fetchrow_array()) {
        print $log_file "fullpath = ". $row[0] . "\n";
        print $log_file "name = ". $row[1] ."\n";
        print $log_file "dir = ". $row[2] ."\n";
        print $log_file "date =  ". $row[3] ."   ". strftime("%d/%m/%Y %H:%M:%S",localtime($row[3]))."\n";
        print $log_file "mark = ". $row[4]."\n\n";
        push @files_to_send, $row[0];
    }
    $st->finish;
    $db->disconnect();
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : --------------------------\n";
}
#------------------------------------------------
sub update_db {
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Update DB starting...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Update DB starting...\n";
    chdir "$folder_origin";
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1}) or print "Failed open db $db_path";
    my @files_=`find * -type f 2>/dev/null`; #add grep for filtering
    foreach my $fname (@files_){
      $fname =~ s/([\r\n])//g;
      my $base = basename("$fname");
      my $dir = dirname("$fname");
      my ($device, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat("$folder_origin/$fname");
      $st=$db->prepare("select date from files where fullpath=?") or print "fail";
      $st->execute($fname);
      my $var1="";
      $var1 = $st->fetchrow();
      if ("$var1" eq ""){
        my $sql_str="insert into files values(?,?,?,?,?)";
        $db->do($sql_str, {}, $fname,$base,$dir,$mtime,"1");
        $var1=$mtime;
      }
      if ("$var1" ne "$mtime"){
        $st=$db->prepare("update files set mark = 1 where fullpath=?");
        $st->execute($fname);
        #$st=$db->prepare("update files set date = ? where fullpath=?");
        #$st->execute($mtime,$fname);

        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : $fname need update\n";
      }
    }
    $st->finish();
    $db->disconnect();
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Update DB completed...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Update DB completed...\n";
}
#------------------------------------------------
sub create_db {
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Create table starting...\n";
    chdir "$folder_origin";
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $db->do("drop table if exists files");
    $db->do("create table files(fullpath text, name text, dir text, date int, mark int)");
    my @files =`find * -type f 2>/dev/null | egrep -i 'Contence.|_etk/cen|_etk/css|ETK/Help' | grep -v -i 'DataBase'`;
    foreach my $fname (@files){
        $fname =~ s/([\r\n])//g;
        my $base = basename("$fname");
        my $dir = dirname("$fname");
        my ($device, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat("$folder_origin/$fname");
        my $sql_str="insert into files values(?,?,?,?,?)";
        $db->do($sql_str, {}, $fname,$base,$dir,$mtime,"0");
    }
    $db->disconnect();
    date_up();
    print $ log_file "$year/$mon/$mday $hour:$min:$sec : Create table completed...\n";
}
#------------------------------------------------
sub create_point_db {
  foreach my $num (@num){
    print "$year/$mon/$mday $hour:$min:$sec : Get file tree of point start...\n";
    my @file_list=`smbclient //$num/base -U $user $pass -c "cd $folder_point; recurse; ls; exit;" | grep -v 'blocks available' | egrep '\\<[\\_a-ZA-Z1-9]' | sed -n '/\\ D\\ /!p' | sed 's/^[ \\t]*//' | sed 's/$folder_point//g' | egrep -v ''Кто-то|товаров.html'`;
    date_up();
    print "$year/$mon/$mday $hour:$min:$sec : Get file tree of point complete...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Create point base start...\n";
    my $lsdir='';
    $dbs=DBI->connect("DBI:SQLite:dbname=$dbs_path","","", {RaiseError => 1});
    $dbs->do("drop table if exists files");
    $dbs->do("create table files(fullpath text, name text, dir text, date int, mark int)");
    foreach my $fname (@file_list){
          $fname =~ s/([\r\n])//g;
          if (index($fname, '\\') >= 0){
              $lsdir=$fname;
              next;
          }
          $lsdir =~ s/[\\]/\//g;
          my $end_string = "$lsdir".'/'."$fname";
          chdir "$folder_origin";
          my $temp_date = `echo "$fname" | awk '{print \$5,\$6,\$7,\$8}'`;
          my $mtime = `date -d "$temp_date" +%s`;
          $fname = `echo "$fname" | awk '{print \$1}'`;
          $end_string = `echo "$end_string" | awk '{print \$1}'`;
          #Надо ограничить выборку файлов...
          if (index ("$end_string", 'Contence.b') >= 0 || index ($end_string, '_etk/css') >= 0 || index ($end_string, 'ETK/Help') >= 0){
              my $sql_str="insert into files values(?,?,?,?,?)";
              $sql_str =~ s/([\r\n])//g;
              $end_string =~ s/([\r\n])//g;
              $end_string =~ s/\///;
              $fname =~ s/([\r\n])//g;
              $lsdir =~ s/([\r\n])//g;
              $mtime =~ s/([\r\n])//g;
              $dbs->do($sql_str, {}, $end_string,$fname,$lsdir,$mtime,"0") or print "fail";
          }
    }
    date_up();
    print "$year/$mon/$mday $hour:$min:$sec : Create point base complete...\n";
    $dbs->disconnect();
  }
}
#------------------------------------------------
sub compare_with_point_db {
  print "$year/$mon/$mday $hour:$min:$sec : Compare started...\n";
  $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
  $sts=$dbs->prepare("select * from files");
  $sts->execute();
  my $update_count=0;
  my @files_for_send;
  while(my @row = $sts->fetchrow_array()) {
      $st=$db->prepare("select date from files where fullpath=?");
      $st->execute("$row[0]");
      my $temp_name=0;
      while ($temp_name = $st->fetchrow()){
          if (scalar($temp_name) < $row[3]){
              $update_count++;
              $st=$db->prepare("update files set mark = 1 where fullpath=?");
              $st->execute("$row[0]");
          }
      }
      $st->finish();
  }
  if ($update_count > 0){
      date_up();
      print "$year/$mon/$mday $hour:$min:$sec : Need update $update_count files.\n";
  }else{
      date_up();
      print "$year/$mon/$mday $hour:$min:$sec : Nothing to update) All good)\n";
  }
  $sts->finish();
  $db->disconnect();
  $dbs->disconnect();
  date_up();
  print "$year/$mon/$mday $hour:$min:$sec : Compare complete...\n";
}
#------------------------------------------------
sub print_help {
  print "\nSimple perl script for sync any local files to many samba IP's from sync_list.\n";
  print "\nUsage: ./my_sync2.pl [options] [IP]\n";
  print "\t\t--help|--usage\tPrint this message;\n";
  print "\t\t--create\tOnly create db of local folder.point;\n";
  print "\t\t--update\tUpdate db of local files and do nothing;\n";
  print "\t\t--compare IP\tCompare local files with files on IP;\n";
  print "\t\t--version\tPrint version;\n";
  print "Without parameters - work as sync all IP' from sync_list\n\n";
}
#------------------------------------------------
sub link_cr {
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $st=$db->prepare("select * from files where mark=1");
    $st->execute();
    while(my @row = $st->fetchrow_array()) {
        `ln -s "$folder_origin/$row[2]/$row[1]" "$folder_dest/$row[2]/$row[1]_new"`;
    }
}
#------------------------------------------------
sub link_del {
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $st=$db->prepare("select * from files where mark=1");
    $st->execute();
    while(my @row = $st->fetchrow_array()) {
        `rm -f "$folder_dest/$row[2]/$row[1]_new"`;
    }
}
#---------------------------------------------------------------------SUBS
unlink $lockfile;
unlink $lockupfile;
exit 0;
