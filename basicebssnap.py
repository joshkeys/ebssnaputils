import boto
from boto.ec2.connection import EC2Connection
from datetime import datetime
import sys
import socket
import logging

#set logging parameters
logfile = open('/var/tmp/snapper.log', 'a')
logfile.close()
logger = logging.getLogger('snapper')
hdlr = logging.FileHandler('/var/tmp/snapper.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Substitute your access key and secret key here
aws_access_key = 'AWSACCESSKEYHERE'
aws_secret_key = 'AWSSECRETKEYHERE'

#if len(sys.argv) < 1:
#    print "Usage: python manage_snapshots.py number_of_snapshots_to_keep description --skiproot"
#    print "Number of snapshots to keep are required. skiproot description is optional"
#    sys.exit(1)

ec2 = EC2Connection(aws_access_key, aws_secret_key)
res = ec2.get_all_instances()
instances = [i for r in res for i in r.instances]
vol = ec2.get_all_volumes()

#Find instance id by matching local ip with private IP listed with Amazon
def findself():
	local_ip = socket.gethostbyname(socket.gethostname())
	for i in instances:
		if i.private_ip_address == local_ip:
			inst_id = i.id
			inst_name = i.__dict__['tags']['Name']
			#print 'Instance found. Private IP is: ',  i.private_ip_address, i.id, i.__dict__['tags']['Name']
			inst_data = [inst_id, inst_name]
			logger.info('Instance found. Private IP is: ' + str(i.private_ip_address))
			return inst_data

#When passed an instance-id getatachedvolumes will find the EBS volumes attached to the instance and return a list of lists [[vol-id, mount-point]]
def getattachedvolumes( target_inst, bool_get_sys_vol=False ):
	target_vols = []
	#print 'Attached Volume ID - Instance ID','-','Device Name'
	for volumes in vol:
		if volumes.attachment_state() == 'attached':
			filter = {'block-device-mapping.volume-id':volumes.id}
			volumesinstance = ec2.get_all_instances(filters=filter)
			ids = [z for k in volumesinstance for z in k.instances]
			for s in ids:
				if s.id == target_inst[0]:
					found_vol = [str(s.id),str(target_inst[1]),str(volumes.id),str(volumes.attach_data.device)]
					logger.info('Found Volume: ' + str(found_vol))
					if (bool_get_sys_vol == False and str(volumes.attach_data.device) == '/dev/sda1'):
						logger.info('Skipping system volume ' + str(volumes.attach_data.device) + ' Vol ID: ' + str(volumes.id))
					else:
						logger.info('Adding volume ' + str(volumes.id) + ' to snap list. It is mounted as ' + str(volumes.attach_data.device)) 
						target_vols.append(found_vol)
					#print target_vols
	return target_vols

#Takes a list of lists in the following format [[vol-id, mount-point]]. Mount point not yet used.
def snapvolumes( target_vols ):
	#vol_ids = []
	#for vol in target_vols:
	#	vol_ids.append(vol[0])
	#print vol_ids
	#snapvols = ec2.get_all_volumes(vol_ids)
	for vol in target_vols:
		#Brute force skip system volume by default in amazon
		#if vol[2] != '/dev/sda1':
		description = vol[1] + ':EC2ID:' + vol[0]+ ':' + vol[3] + ' ' + 'EBS:' + vol[2] + ' Stamp: ' + datetime.today().isoformat(' ')
		if len(sys.argv) > 2:
    			description = sys.argv[2]
		vol_list = ec2.get_all_volumes(vol[2])
		volume = vol_list[0]
		if volume.create_snapshot( description ):
    			logger.info('Snapshot created with description: ' + description)	

def rollsnaps( target_vols, snap_keeps ):
	counter = 0
	for vol in target_vols:
		vol_list = ec2.get_all_volumes(vol[2])
		vol_snaps = vol_list[0].snapshots()
		#print len(vol_snaps)
		if len(vol_snaps) <= snap_keeps:
			logger.info(str(vol_list[0]) + ' has fewer or an equal amount of snaps, ' + str(len(vol_snaps)) + ' than the amount specfied to keep, ' + str(snap_keeps) + '. Keeping all snaps.')
                elif len(vol_snaps) > snap_keeps:
                        num_snaps_todel = len(vol_snaps) - snap_keeps
			logger.info(str(vol_list[0]) + ' has more snaps, ' + str(len(vol_snaps)) + ' than the amount specfied to keep, ' + str(snap_keeps) + '. Removing ' + str(num_snaps_todel) + ' snaps.')
			for i in range(num_snaps_todel):
				vol_snaps[i].delete() 
				logger.info('Deleteing snap index ' + str(i) + ' snap id: ' + str(vol_snaps[i].id))			
						

		#logger.info(str(vol_list[0]) + ' has the following snaps. ' + str(vol_snaps) + ' Time: ' + str(vol_snaps[counter].start_time))
		#for tsnap in vol_snaps:
		#	logger.info(str(vol[0]) + ' ' + str(vol[1]) + ' Snap: ' + str(tsnap.id) + ' Time: ' + str(tsnap.start_time)) 	 


##########################################	


backed_vols = getattachedvolumes(findself())
snapvolumes(backed_vols)
rollsnaps(backed_vols, 3)
