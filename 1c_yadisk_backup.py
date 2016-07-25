#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import datetime
import requests
import logging
import os
import sys
import platform
import subprocess
requests.packages.urllib3.disable_warnings()
# ---------- SETTINGS -------------

# Папка для сохранения архивов с бекапами. После отправки на яндекс диск архивы будут удалены.
BACKUPDIR="/tmp/1c_backup/"

# Префикс для понимания что вообще за бекапы у нас лежат на яндекс диске
PREFIX="UT11"

# Переменная дата для создания архива используется в имени файлов
CURRENTDATETIME=datetime.datetime.now().strftime("%d-%m-%Y--%H-%M")

# ---------- SETTINGS BACKUP FILES-------------
# Указываем папки для архивации, все они будут помещены в один архив и отправлены на яндекс диск
BACKUP_DIRECTORIES=['/home/starsoft/UT11demo', '/etc']

# Имя файла бекапа
ARCHIVEFILENAME = '%s-%s.tar.gz' % (CURRENTDATETIME, PREFIX)

# Полный путь до него
ARCHIVEPATH = BACKUPDIR + ARCHIVEFILENAME

# ---------- SETTINGS YANDEX API-------------
# Yandex.Disk токен (как получить токен, написано в инструкции)
TOKEN=''

# Как долго хранить бекапы на Яндекс диске (в днях)
STOREPERIODINDAYS = 2

# ---------- FUNCTIONS ---------------------
def createArchive(directories, archivePath):
	"""
	Archiving list of directories to file
	"""
	import tarfile
	if os.path.isfile(archivePath):
		logging.info(u'File %s exists. Removing...' % (archivePath))
		os.remove(archivePath)

	with tarfile.open(archivePath, "w:gz") as tar:
		for directory in directories:
			if os.path.isfile(directory) or os.path.isdir(directory):
				tar.add(directory, arcname=archivePath)
			else:
				logging.info(u"Path %s doesn't exists. Skipping..." % (directory))

def checkJsonErrors(data):
	"""
	Parsing JSON output and log errors if exists.
	"""
	if 'error' in data.keys():
		msg = u"Can't perform operation. Reason: %s." % (data['message'].decode("utf-8"))
		print msg
		logging.error(msg)
		sys.exit(1)

def getUploadUrl(filename):
	"""
	Before uploading file to Yandex.Disk we must get exact URL with path for future uploaded file.
	This function perform this operation.
	"""
	import sys
	reload(sys)
	sys.setdefaultencoding('utf-8')
	filepath = filename
	filename = os.path.basename(filepath)
	url = "https://cloud-api.yandex.net:443/v1/disk/resources/upload/?path=app:/%s&overwrite=true" % (filename)
	headers = {'Authorization': 'OAuth %s' % (TOKEN)}
	logging.info(u'Trying to get URL for upload file %s tu Yandex.Disk.' % (filename))
	response = requests.get(url,headers=headers)
	try:
		data = response.json()
	except:
		pass
	else:
		checkJsonErrors(data)
		return data['href']
	msg = u'Unknown error while getting upload URL.'
	print msg
	logging.error(msg)
	sys.exit(1)

def uploadFIle(filename):
	"""
	Function for upload file to Yandex.Disk.
	"""

	file = str(filename)
	uploadUrl = getUploadUrl(file)
	headers = {'Authorization': 'OAuth %s' % (TOKEN)}
	files = {'file': open(file, 'rb')}
	logging.info(u'Trying to upload file %s to Yandex.Disk.' % (file))
	response = requests.post(uploadUrl,headers=headers,files=files)

	try:
		data = response.json()
	except:
		pass
	else:
		checkJsonErrors(data)

	# documentation: https://tech.yandex.ru/disk/poligon/#!//v1/disk/resources/UploadExternalResource
	if response.status_code not in ['201', '202']:
		logging.info(u'File %s successfully uploaded.' % (file))
	else:
		msg = u'Some error while uploading file %s. Error code: %s. Error text: %s.' % (file, response.status_code, response.text)
		print msg
		logging.error(msg)
		sys.exit(1)

def createDirectories(dirList):
	"""
	Recursively create directories.
	"""
	import errno 
	for directory in dirList:
		try:
			os.makedirs(directory)
		except OSError as exc:
			if exc.errno == errno.EEXIST and os.path.isdir(directory):
				pass
			else:
				raise

def getUploadedFiles():
	"""
	Return list of previosly uploaded archives.
	"""

	url = "https://cloud-api.yandex.net:443/v1/disk/resources/last-uploaded?media_type=compressed"
	headers = {'Authorization': 'OAuth %s' % (TOKEN)}
	response = requests.get(url,headers=headers)

	try:
		data = response.json()
	except:
		pass
	else:
		checkJsonErrors(data)
		return data['items']
	msg = u'Unknown error while getting uploaded files.'
	print msg
	logging.error(msg)
	sys.exit(1)

def removeFileFromYandexDisk(filepath):
	"""
	Remove giving file from Yandex.Disk.
	"""
	url = "https://cloud-api.yandex.net:443/v1/disk/resources?path=%s&permanently=true" % (filepath)
	headers = {'Authorization': 'OAuth %s' % (TOKEN)}
	logging.info(u'Trying to remove file %s from Yandex.Disk.' % (filepath))
	response = requests.delete(url,headers=headers)
	try:
		data = response.json()
	except:
		pass
	else:
		checkJsonErrors(data)

	# documentation: https://tech.yandex.ru/disk/poligon/#!//v1/disk/resources/DeleteResource
	if response.status_code in [202, 204]:
		logging.info(u'File %s was deleted from Yandex.Disk.' % (filepath))
	else:
		msg = u'Some error occured while removing %s. Status code: %s. Response text: %s.' % (filepath, response.status_code, response.text)
		print msg
		logging.error(msg)
		sys.exit(1)

def removeOldFilesFromYandexDisk(days=999):
	"""
	Remove files older then given days
	"""
	import dateutil.parser
	import pytz

	oldestdate = datetime.datetime.utcnow().replace(tzinfo = pytz.utc) - datetime.timedelta(days=days)
	files = getUploadedFiles()
	for file in files:
		if dateutil.parser.parse(file['created']) < oldestdate:
			removeFileFromYandexDisk(file['path'])

def stop1cService():
	"""
	Stoping 1c server service. If service (usually named srv1cv83) not gracedully stopped, then we try to kill them manually.
	"""
	import platform
	import psutil
	import subprocess
	distrName = platform.linux_distribution()[0]
	if 'ubuntu' in distrName.lower():
		pipe = subprocess.Popen('service srv1cv83 stop', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		logging.info(u'Stopping 1c server service...')
		for proc in psutil.process_iter():
			try:
				pinfo = proc.as_dict(attrs=['pid', 'name'])
			except psutil.NoSuchProcess:
				pass
			else:
				if pinfo['name'] in ['ragent', 'rmngr', 'rphost']:
					p = psutil.Process(pinfo['pid'])
					p.terminate()

def start1cService():
	"""
	Starting 1c server service (usually named srv1cv83).
	"""
	distrName = platform.linux_distribution()[0]
	if 'ubuntu' in distrName.lower():
		pipe = subprocess.Popen('service srv1cv83 start', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		if pipe.wait() == 0:
			logging.info(u'Server 1C has been started.')
		else:
			msg = u'Some error occured while starting 1C Server service.'
			logging.error(msg)
			print msg
			sys.exit(1)

def removeTempFiles(tempDir):
	"""
	Remove temp archives, which were created in previous backups.
	"""
	fileList = [ f for f in os.listdir(tempDir) if f.endswith(".tar.gz") ]
	for file in fileList:
		filepath = tempDir + file
		logging.info(u'Remove temp file %s.' % (filepath))
		os.remove(filepath)

if __name__ == "__main__":

	# check if we not sudo
	if os.getuid() != 0:
		print "This program is not run as sudo or elevated this. Please rerun this script with sudo priviledges."
		sys.exit(1)

	# create temp dirs
	createDirectories([BACKUPDIR])
	# create log file
	try:
		with open(BACKUPDIR + 'backup.log', 'a'):
			pass
	except:
		print "Can't create logfile."
		sys.exit(1)

	# set logging configuration
	logging.basicConfig(format = u'%(asctime)s %(levelname)-8s %(message)s', level = logging.INFO, filename = str(BACKUPDIR + 'backup.log'))
	
	# stopping 1c service
	stop1cService()

	# create archive
	createArchive(BACKUP_DIRECTORIES, ARCHIVEPATH)

	# starting 1c service
	start1cService()

	# upload file to yandex disk
	uploadFIle(ARCHIVEPATH)

	# delete files older then 3 days
	removeOldFilesFromYandexDisk(days=STOREPERIODINDAYS)

	# clear temp files
	removeTempFiles(BACKUPDIR)