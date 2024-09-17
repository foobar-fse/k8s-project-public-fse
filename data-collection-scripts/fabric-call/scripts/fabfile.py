from fabric.api import *

# Note: replace <username> with your user name.

# variables use underline(_), paths use dash (-)
# both key and val use singular(i.e. k8s_api_resource)  
k8s_deploy = ['172.16.112.64', '172.16.112.37', '172.16.112.38', '172.16.112.39',
            '172.16.112.40', '172.16.112.49', '172.16.112.50', '172.16.112.51',
            '172.16.112.52', '172.16.112.53', '172.16.112.54', '172.16.112.55',
            '172.16.112.56', '172.16.112.57', '172.16.112.58', '172.16.112.59',
            '172.16.112.60', '172.16.112.61', '172.16.112.62', '172.16.112.63',
            '172.16.112.71', '172.16.112.72', '172.16.112.73', '172.16.112.74']

#k8s_deploy = k8s_deploy[1:3]

env.roledefs = {
        'k8s_deploy': k8s_deploy,
        'cpu_ram': k8s_deploy, # cpu and ram metric
        'disk': k8s_deploy, # local disk metric
        'docker': k8s_deploy, # master + worker node
        'image': k8s_deploy,
        'iptable': k8s_deploy,
        'interface': k8s_deploy,
        'calico_resource': ['172.16.112.37'], # calicoctl both pod and node installed 
        'mount': k8s_deploy, # mount info, PD-PV/PVC mapping
        'persistent_disk': ['172.16.112.85'], # node-85, previously in node-63  
        'k8s_api_resource': ['172.16.112.64']# node-64, master-node
}


nfs_pd_path = '/mnt/k8s_nfs_pv/' # for persistent_disk and pd_content
base_dir = '/home/<username>/data-collection-6/' # local store, and move to 172.16.112.1 periodically 

outpaths = {
        'cpu_ram': base_dir + '/metrics/cpu-ram/', # metrics
        'disk': base_dir + '/metrics/local-disk/',
        'docker': base_dir + '/docker-state/', # states
        'image': base_dir + '/image-state/',
        'iptable': base_dir + '/network-state/iptable/',
        'interface': base_dir + '/network-state/interface/',
        'calico_resource': base_dir + '/network-state/calico-resource/',
        'mount': base_dir + '/storage-state/mount/',
        'persistent_disk': base_dir + '/storage-state/persistent-disk/',
        'k8s_api_resource': base_dir + '/k8s-api-resource/'

}

scripts_v2 = {
            'cpu_ram': outpaths['cpu_ram'] + '/scripts/python2/' + 'cpu_ram_polling.py', # metrics
            'cpu_ram_raw': outpaths['cpu_ram'] + '/scripts/python2/' + 'cpu_ram_polling_raw.py',
            'disk': outpaths['disk'] + '/scripts/python2/' + 'disk_polling.py',
            'disk_raw': outpaths['disk'] + '/scripts/python2/' + 'disk_polling_raw.py',
            'docker': outpaths['docker'] + '/scripts/python2/' + 'docker_state_polling.py', # states
            'image': outpaths['image'] + '/scripts/python2/' + 'image_state_polling.py',
            'iptable': outpaths['iptable'] + '/scripts/python2/' + 'iptables_polling.py',
            'iptable_raw': outpaths['iptable'] + '/scripts/python2/' + 'iptables_polling_raw.py', # raw means without parsing
            'interface': outpaths['interface'] + '/scripts/python2/' + 'interfaces_polling.py',
            'interface_raw': outpaths['interface'] + '/scripts/python2/' + 'interfaces_polling_raw.py',
            'calico_resource': outpaths['calico_resource'] + '/scripts/python2/' + 'calico_resources_polling.py',
            'mount': outpaths['mount'] + '/scripts/python2/' + 'mount_polling.py',
            'mount_raw': outpaths['mount'] + '/scripts/python2/' + 'mount_polling_raw.py',
            'persistent_disk': outpaths['persistent_disk'] + '/scripts/python2/' + 'persistent_disk_state_polling.py',
            'k8s_api_resource': outpaths['k8s_api_resource'] + '/scripts/python2/' + 'k8s_api_resources_polling.py'
}

scripts_v3 = {
            'cpu_ram': outpaths['cpu_ram'] + '/scripts/python3/' + 'cpu_ram_polling_3.py', # metrics
            'cpu_ram_raw': outpaths['cpu_ram'] + '/scripts/python3/' + 'cpu_ram_polling_raw_3.py',
            'disk': outpaths['disk'] + '/scripts/python3/' + 'disk_polling_3.py',
            'disk_raw': outpaths['disk'] + '/scripts/python3/' + 'disk_polling_raw_3.py',
            'docker': outpaths['docker'] + '/scripts/python3/' + 'docker_state_polling_3.py', # states
            'image': outpaths['image'] + '/scripts/python3/' + 'image_state_polling_3.py',
            'iptable': outpaths['iptable'] + '/scripts/python3/' + 'iptables_polling_3.py',
            'iptable_raw': outpaths['iptable'] + '/scripts/python3/' + 'iptables_polling_raw_3.py',
            'interface': outpaths['interface'] + '/scripts/python3/' + 'interfaces_polling_3.py',
            'interface_raw': outpaths['interface'] + '/scripts/python3/' + 'interfaces_polling_raw_3.py',
            'calico_resource': outpaths['calico_resource'] + '/scripts/python3/' + 'calico_resources_polling_3.py',
            'mount': outpaths['mount'] + '/scripts/python3/' + 'mount_polling_3.py',
            'mount_raw': outpaths['mount'] + '/scripts/python3/' + 'mount_polling_raw_3.py',
            'persistent_disk': outpaths['persistent_disk'] + '/scripts/python3/'  + 'persistent_disk_state_polling_3.py',
            'k8s_api_resource': outpaths['k8s_api_resource'] + '/scripts/python3/' + 'k8s_api_resources_polling_3.py'
            }



#@parallel
@roles('k8s_deploy')
def test_k8s_task():
    with settings(user='<username>', port=1022, use_sudo=True, timeout=60):
        run('scp -P 1022 -r <username>@k8sdeploy-n37:~/data-collection-6 /home/<username>/') 
        #run('hostname; sudo apt install vim -y; exit')
        #run("ssh -p 1022 <username>@k8sdeploy-n37 'hostname; exit'")
        #run('hostname; crontab -l | tail -n 4')
        #run('hostname; top -n1 | head -n 10')
        #run('hostname; find ~/data-collection-5/ -type d  -ctime -3 -name 2020*  -exec tar cvf {}.tar.gz {} \; ')
        #run('hostname; mount | grep k8s_nfs_pv')
        #run('hostname; export CALICO_DATASTORE_TYPE=kubernetes; export CALICO_KUBECONFIG=~/.kube/config;  calicoctl  get node')

#@parallel
@roles('k8s_deploy')
def test_report_size():
    with settings(user='<username>', port=1022, use_sudo=True, timeout=60):
        run('du -sh ~/data-collection-5; exit')


## define tasks

# polling the cpu+ram metric in each node (include k8s-master)
@parallel
@roles('cpu_ram')
def cpu_ram_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['cpu_ram'] ,'outpath': outpaths['cpu_ram']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['cpu_ram'] ,'outpath': outpaths['cpu_ram']}
        print cmd
        run(cmd)

# raw polling without parse 
@parallel
@roles('cpu_ram')
def cpu_ram_raw_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v2['cpu_ram_raw'], 'outpath': outpaths['cpu_ram']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v3['cpu_ram_raw'], 'outpath': outpaths['cpu_ram']}
        print cmd
        run(cmd)

# polling the local disk metric in each node (include k8s-master)
@parallel
@roles('disk')
def disk_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['disk'] ,'outpath': outpaths['disk']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['disk'] ,'outpath': outpaths['disk']}
        print cmd
        run(cmd)

# raw polling without parse 
@parallel
@roles('disk')
def disk_raw_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v2['disk_raw'], 'outpath': outpaths['disk']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v3['disk_raw'], 'outpath': outpaths['disk']}
        print cmd
        run(cmd)


# polling the docker running state in each node (include k8s-master, for kube-system docker)
@parallel
@roles('docker')
def docker_polling_task():  
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['docker'] ,'outpath': outpaths['docker']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['docker'] ,'outpath': outpaths['docker']}
        print cmd
        run(cmd)

# polling the docker image state in each node (also include k8s-master)
@parallel
@roles('image')
def image_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['image'], 'outpath': outpaths['image']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['image'], 'outpath': outpaths['image']}
        print cmd
        run(cmd)

# network state consist of iptables + interfaces + calico_resources (+ calico_node_ipam)
# polling the iptables (i.e. filter/nat tables) in each node (include k8s-master)
@parallel
@roles('iptable')
def iptable_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):        
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['iptable'], 'outpath': outpaths['iptable']} 
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['iptable'], 'outpath': outpaths['iptable']}
        print cmd
        run(cmd)

# raw polling without parse
@parallel
@roles('iptable')
def iptable_raw_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
    # use cripts['iptable_raw'] without parsing
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v2['iptable_raw'], 'outpath': outpaths['iptable']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s'% {'script': scripts_v3['iptable_raw'], 'outpath': outpaths['iptable']}
        print cmd
        run(cmd)


# polling the network interfaces in each node (include k8s-master)
# use sudo is much faster, even if not required
@parallel
@roles('interface')
def interface_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['interface'], 'outpath': outpaths['interface']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['interface'], 'outpath': outpaths['interface']}
        print cmd
        run(cmd)

# raw polling without parse
@parallel
@roles('interface') 
def interface_raw_polling_task():
    with settings(user='<username>', port=1022, use_sudo=True):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['interface_raw'], 'outpath': outpaths['interface']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['interface_raw'], 'outpath': outpaths['interface']}
        print cmd
        run(cmd)

#polling the calico resources
# run in a node/pod with calicoctl, not need parallel  
@roles('calico_resource')
def calico_resource_polling_task():
    with settings(user='<username>', port=1022): # how to read ~/.bashrc env?    
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s -f %(format)s'\
                    %{'script': scripts_v2['calico_resource'], 'outpath': outpaths['calico_resource'], 'format': 'json'}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s -f %(format)s'\
                    %{'script': scripts_v3['calico_resource'], 'outpath': outpaths['calico_resource'], 'format': 'json'}
        print cmd
        #run('source ~/.bashrc;  ' + cmd)
        run(cmd)

# polling the mount info in each node (include k8s-master)
@roles('mount')
def mount_polling_task():
    with settings(user='<username>', port=1022):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['mount'], 'outpath': outpaths['mount']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['mount'], 'outpath': outpaths['mount']}
        print cmd
        run(cmd)

# raw polling without parse
@roles('mount')
def mount_raw_polling_task():
    with settings(user='<username>', port=1022):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v2['mount_raw'], 'outpath': outpaths['mount']}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s' % {'script': scripts_v3['mount_raw'], 'outpath': outpaths['mount']}
        print cmd
        run(cmd)

# polling the persistent disk state in NFS
# run in a node with NFS access, i.e. node-63, node-85
@roles('persistent_disk') 
def persistent_disk_polling_task():
    with settings(user='<username>', port=1022):  
        output = run('python --version')
        if 'Python 2.7' in output: 
            cmd = 'python %(script)s -i %(inpath)s -o %(outpath)s'\
                %{'script': scripts_v2['persistent_disk'], 'outpath': outpaths['persistent_disk'], 'inpath': nfs_pd_path}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -i %(inpath)s -o %(outpath)s'\
                %{'script': scripts_v3['persistent_disk'], 'outpath': outpaths['persistent_disk'], 'inpath': nfs_pd_path}
        print cmd
        run(cmd)

# polling the k8s-api-resources 
# only run in a node with kubectl, and k8s-master is the best place
@roles('k8s_api_resource')
def k8s_api_resource_polling_task():
    with settings(user='<username>', port=1022):
        output = run('python --version')
        if 'Python 2.7' in output:
            cmd = 'python %(script)s -o %(outpath)s -f %(format)s'\
                %{'script': scripts_v2['k8s_api_resource'], 'outpath': outpaths['k8s_api_resource'], 'format': 'json'}
        elif 'Python 3.6' in output:
            cmd = 'python %(script)s -o %(outpath)s -f %(format)s'\
                %{'script': scripts_v3['k8s_api_resource'], 'outpath': outpaths['k8s_api_resource'], 'format': 'json'}
        print cmd
        run(cmd)
