#!/usr/bin/perl -w
use POSIX;
use strict;
use Path::Class;
use DBI;
use Switch;
use Parse::Pidl;
use File::Copy;
#use SMB;
use File::Basename;

my $version="2.6.3";

my $db_path="/home/defender/.my_sync/files.db";
my $dbs_path="/home/defender/.my_sync/shop.db";
my $db;
my $dbs;
my $st;
my $st_rem;
my $sts;
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) ="";

my $home_dir=dir("/home/defender/.my_sync/");
my $u_dir=dir("/windows/u/Domino8/mail/PLAN Lin/my_sync/");
my $stop_file='/windows/u/Domino8/mail/PLAN Lin/my_sync/stop/my_sync';
my $sync_list='/windows/u/Domino8/mail/PLAN Lin/my_sync/sync_list';

my $errlog_end='/windows/u/Domino8/PROJECT.update/my_sync.err';
my $log_end='/windows/u/Domino8/PROJECT.update/my_sync.log';
my $fileslog_end='/windows/u/Domino8/PROJECT.update/my_sync_files.log';

my $errlog=$home_dir->file("my_sync.err");
open (my $errlog_file, '>>', "$errlog") or print "fail";
my $log=$home_dir->file("my_sync.log");
open (my $log_file, '>>', "$log") or print $errlog_file "fail";
my $fileslog=$home_dir->file("my_sync_files.log");
open (my $fileslog_file, '>>', "$fileslog") or print $errlog_file "fail";

my $config_file = "/windows/u/Domino8/mail/PLAN Lin/my_sync/my_sync.conf";

my $folder_dest='/home/defender/.csync/PROJECT.shop/';
my $folder_origin='/windows/u/Domino8/PROJECT.shop/';
my $folder_shop='Domino8\PROJECT';

my @num='';

my $user='semkin';
my $pass='kbkbv';
my $var1=0;

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
            print "\nSimple perl script for sync any local files to many samba IP's from sync_list.\n";
            print "\nUsage: ./my_sync2.pl [options] [IP]\n";
            print "\t\t--help|--usage\tPrint this message;\n";
            print "\t\t--create\tOnly create db of local PROJECT.shop;\n";
            print "\t\t--update\tUpdate db of local files and do nothing;\n";
            print "\t\t--compare IP\tCompare local files with files on IP;\n";
            print "\t\t--version\tPrint version;\n";
            print "Without parameters - work as sync all IP' from sync_list\n\n";
            exit;
        }
        case "--create"{
            create_db();
            exit;
          }
        case "--update"{
            update_db();
            exit;
          }
        case "--compare"{
            if (! $ARGV[1])
            { print "Need IP!!!!!"; exit;}
            @num = $ARGV[1];
            create_shop_db();
            compare_with_shop_db();
            show_files();
            send_files();
            exit;
          }
        case "--version"{
            print "my_sync2, version: $version.\n";
            exit;
          }
        else {
            print "\nSimple perl script for sync any local files to many samba IP's from sync_list.\n";
            print "\nUsage: ./my_sync2.pl [options] [IP]\n";
            print "\t\t--help|--usage\tPrint this message;\n";
            print "\t\t--create\tOnly create db of local PROJECT.shop;\n";
            print "\t\t--update\tUpdate db of local files and do nothing;\n";
            print "\t\t--compare IP\tCompare local files with files on IP;\n";
            print "\t\t--version\tPrint version;\n";
            print "Without parameters - work as sync all IP' from sync_list\n\n";
            exit;
        }
    }
}

date_up();
print $log_file "$year/$mon/$mday $hour:$min:$sec : Synchronization by perl2 started\n";
print "$year/$mon/$mday $hour:$min:$sec : Synchronization by perl2 started\n";

#-----------------------------------Проверка дисков сетевых---------------------
my $diru = dir("/windows/u/");
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
  return;
}
#-------------------------------------------------------------------------------

my $count_num=`cat $sync_list | wc -l`;

@num=`cat $sync_list`;
date_up();
print "$year/$mon/$mday $hour:$min:$sec : IP's for sync:\n";
foreach my $num (@num)
{
  $num =~ s/([\r\n])//g;
  print "$year/$mon/$mday $hour:$min:$sec : $num\n";
}

my @num_norm;
foreach my $num (@num)
{
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Check host $num - ";
    if ( `ping $num -c 2 > /dev/null; echo $?` == 0 )
    {
        push @num_norm, $num;
        print $log_file "OK\n";
    }
    {
      date_up();
      print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Sadly, no ping to IP $num...\n";
      print "fail\n";
      next;
    }
}

#Uncomment for tests:
#@num_norm='192.168.35.252';
chdir "$folder_origin";

my $i=0;

update_db();
my @files_to_send;
show_files();
my $size = @files_to_send;
if ( $size == 0 )
{
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Nothing to do...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Nothing to do...\n";
    `cp -u $log $log_end`;
    exit 0;
}else
{
  send_files();
}

date_up();
print $log_file "$year/$mon/$mday $hour:$min:$sec : Synchronization by perl2 complete\n";
print "$year/$mon/$mday $hour:$min:$sec : Synchronization by perl2 complete\n";
`cp -u $errlog $errlog_end`;
`cp -u $log $log_end`;
`cp -u $fileslog $fileslog_end`;
#---------------------------------------------------------------------SUBS
sub send_files {
    my $result = 0;
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    date_up();
    print $log_file "$mday/$mon/$year $hour:$min:$sec : Send started...\n";
    $st=$db->prepare("select * from files where mark=1");
    $st->execute();
    while(my @row = $st->fetchrow_array()) {
        foreach my $num (@num_norm){
            my $dir = "$row[2]";
            $dir =~ s/\//\\/g;
            `ln -s "$folder_origin$row[2]/$row[1]" "$folder_dest$row[2]/$row[1]_new"`;
            chdir "$folder_dest$row[2]";
            `smbclient //$num/base -U $user $pass -c "cd $folder_shop\\$row[2]; prompt; mput "$row[1]_new"; exit"`;
            `smbclient //$num/base -U $user $pass -c "cd $folder_shop\\$row[2]; prompt; del "$row[1]"; exit"`;
            my $ls=`smbclient //$num/base -U $user $pass -c "cd $folder_shop\\$row[2]; ls "$row[1]"; exit" | wc -l`;
            if ($ls eq 3){
                $result=1;
                date_up();
                print $errlog_file "$year/$mon/$mday $hour:$min:$sec : Deleting file $row[0] has been failed for $num.";
                print $errlog_file "$year/$mon/$mday $hour:$min:$sec : File will be send again later.";
            }else{
                if (! $result eq 1){
                    $result=1;
                }else{$result=0;}
            }
            `smbclient //$num/base -U $user $pass -c "cd $folder_shop\\$row[2]; prompt; rename "$row[1]_new" "$row[1]"; exit"`;
            `rm -f "$row[1]_new"`;
            date_up();
            print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : File $row[0] PUSHED to $num...\n";
            print "$year/$mon/$mday $hour:$min:$sec : File $row[0] PUSHED to $num...\n";
            chdir "$folder_origin";

        }
        if ($result eq 0){
            $st_rem=$db->prepare("update files set mark = 0 where fullpath=?") or print $errlog_file "update failed(((";
            $st_rem->execute($row[0]) or print $errlog_file "update failed(((";
        }
    }
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Send completed...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Send completed...\n";
    $st_rem->finish();
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
    print $log_file "$year/$mon/$mday $hour:$min:$sec : -----------------------------------------------\n";
}
#------------------------------------------------
sub update_db {
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Update DB starting...\n";
    chdir "$folder_origin";
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    my @files=`find * -type f 2>/dev/null | egrep -i 'Contence.|_etk/cen|_etk/css|ETK/Help' | grep -v -i 'DataBase'`;
    foreach my $fname (@files)
    {
      $fname =~ s/([\r\n])//g;
      my $base = basename("$fname");
      my $dir = dirname("$fname");
      my ($device, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat("$folder_origin/$fname");
      $st=$db->prepare("select date from files where fullpath=?");
      $st->execute($fname);
      my $var1="";
      $var1 = $st->fetchrow();
      if ("$var1" eq "")
      {
        my $sql_str="insert into files values(?,?,?,?,?)";
        $db->do($sql_str, {}, $fname,$base,$dir,$mtime,"1");
        $var1=$mtime;
      }
      if ("$var1" ne "$mtime")
      {
        $st=$db->prepare("update files set mark = 1 where fullpath=?");
        $st->execute($fname);
        $st=$db->prepare("update files set date = ? where fullpath=?");
        $st->execute($mtime,$fname);

        print $fileslog_file "$year/$mon/$mday $hour:$min:$sec : $fname need update\n";
      }

    }
    $st->finish;
    $db->disconnect();
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Update DB completed...\n";
}
#------------------------------------------------
sub create_db {
    date_up();
    print $log_file "$year/$mon/$mday $hour:$min:$sec : Create table starting...\n";
    chdir "$folder_origin";
    $db=DBI->connect("DBI:SQLite:dbname=$db_path","","", {RaiseError => 1});
    $db->do("drop table if exists files");
    $db->do("create table files(fullpath text, name text, dir text, date int, mark int)");
    my @files=`find * -type f 2>/dev/null | egrep -i 'Contence.|_etk/cen|_etk/css|ETK/Help|ETK/version.txt' | grep -v -i 'DataBase' | sort`;
    foreach my $fname (@files)
    {
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
sub create_shop_db {
  foreach my $num (@num_norm){
    print "$year/$mon/$mday $hour:$min:$sec : Get file tree of shop start...\n";
    my @file_list=`smbclient //$num/base -U $user $pass -c "cd Domino8\\PROJECT; recurse; ls; exit;" | grep -v 'blocks available' | egrep '\\<[\\_a-ZA-Z1-9]' | sed -n '/\\ D\\ /!p' | sed 's/^[ \\t]*//' | sed 's/\\\\Domino8\\\\PROJECT//g' | egrep -v 'Кто-то|товаров.html'`;
    date_up();
    print "$year/$mon/$mday $hour:$min:$sec : Get file tree of shop complete...\n";
    print "$year/$mon/$mday $hour:$min:$sec : Create shop base start...\n";
    my $lsdir='';
    $dbs=DBI->connect("DBI:SQLite:dbname=$dbs_path","","", {RaiseError => 1});
    $dbs->do("drop table if exists files");
    $dbs->do("create table files(fullpath text, name text, dir text, date int, mark int)");
    foreach my $fname (@file_list)
    {
          $fname =~ s/([\r\n])//g;
          if (index($fname, '\\') >= 0)
          {
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
          if (index ("$end_string", 'Contence.b') >= 0 || index ($end_string, '_etk/cen') >= 0 || index ($end_string, '_etk/css') >= 0 || index ($end_string, 'ETK/Help') >= 0){
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
    print "$year/$mon/$mday $hour:$min:$sec : Create shop base complete...\n";
    $dbs->disconnect();
  }

}
sub compare_with_shop_db {
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
#---------------------------------------------------------------------SUBS
exit 0;
