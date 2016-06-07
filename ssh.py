"""
module for running commands remotely on ec2 instances
"""
import paramiko


def run_command(address, command):
    """
    run a command on the remote ec2 machine via ssh
    @param address: ip address of remote machine
    @param command: shell command to run
    @return: bool if command succeeded
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(address, username="ec2-user")
        ssh_in, ssh_out, ssh_err = ssh.exec_command(command)
        exit_status = ssh_out.channel.recv_exit_status()
        return exit_status == 0
    finally:
        ssh.close()


def stop_all_containers(address, timeout):
    """
    stop all docker containers on the remote ec2 machine
    @param address:: ip address of remote machine
    @param timeout: number of seconds to wait for containers to stop
    @return: booll if command succeeded
    """
    command = "docker stop -t %d $(docker ps -a -q)" % (timeout)
    return run_command(address, command)
