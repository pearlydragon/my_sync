# my_sync
one-directional smb sync
This script created for sync 1 local folder with too many smb folder, if you cant mount 100 folders for sync.
Yes me need sync 100 point via smb, and cant use ssh(((
All simple - at first run take local folder, find * and parse output in sqlite db. At second run - compare mtime files in folder and in db. If file more yung, than in db - mark it for transfer, write his mtime in db.
Later we create _new link to file for send and send link. Delete file from smb, rename _new file to file. It need for minimize time to change file. If samba cant delete file from one more point - sync send this file again in next run.
