import sys
sys.path.append(f'/vol0206/data/ra010014/u13266/workflow/psij/psij-python_pjsub/src')

import cloudpickle
import pickle
import psij
# import shutil

from pathlib import Path
from datetime import timedelta
from typing import Optional, Union, Dict

class psij_ext:

    def __init__( self, instance, work_directory = None ):
        self.job_executor = psij.JobExecutor.get_instance( instance )

        # ToDo: Need to check the work_directory exist before using it
        if work_directory is not None:
            self.job_executor.work_directory = work_directory
        self.work_directory = self.job_executor.work_directory

    def python_serialize_function_and_args( self, filename, func_obj, args = [], kwargs = {} ) -> None:
        with open( filename, 'wb' ) as obj_file:
            cloudpickle.dump( ( func_obj, args, kwargs ), obj_file )

    def python_execute_script( self, func_obj_path, result_path ) -> str:
        return f"""
import pickle
import sys

with open( "{func_obj_path}", 'rb' ) as obj_file:
    func_obj, args, kwargs = pickle.load( obj_file )

results = None
exception = None

try:
    results = func_obj( *args, **kwargs )
except Exception as e:
    exception = e

with open( "{result_path}", 'wb' ) as res_file:
    pickle.dump( (results, exception), res_file)
"""

    def config_spec( 
        self, 
        # work_directory,
        arguments: list[str],
        executable: str,
        directory: Union[str, Path, None] = None,
        name: Optional[str] = None,
        inherit_environment: bool = True,
        environment: Optional[Dict[str, Union[str, int] ] ] = None,
        stdin_path: Union[str, Path, None] = None,
        stdout_path: Union[str, Path, None] = None,
        stderr_path: Union[str, Path, None] = None,
        pre_launch: Union[str, Path, None] = None,
        post_launch: Union[str, Path, None] = None,
        launcher: Optional[str] = None,
        node_count: Optional[int] = None,
        process_count: Optional[int] = None,
        processes_per_node: Optional[int] = None,
        cpu_cores_per_process: Optional[int] = None,
        gpu_cores_per_process: Optional[int] = None,
        exclusive_node_use: bool = False,
        memory: Optional[int] = None,
        duration: Optional[timedelta] = None,
        queue_name:Optional[str] = None,
        account: Optional[str] = None,
        reservation_id: Optional[str] = None,
        custom_attributes: Optional[ Dict[str,object] ] = None
    ) -> psij.JobSpec:
        spec = psij.JobSpec()

        # spec.executable = '/home/users/s.pornmaneerattanatri/.pyenv/versions/workflow_dev/bin/python'

        # spec.arguments = [ f'{work_directory}/{job.id}.py' ]

        spec.executable = executable

        spec.arguments = arguments

        if directory != None: 
            spec.directory = directory

        if name != None:
            spec.name = name

        if inherit_environment:
            spec.inherit_environment = inherit_environment 

        if environment != None:
            spec.environment = environment

        if stdin_path != None:
            spec.stdin_path = stdin_path

        if stdout_path != None:
            spec.stdout_path = stdout_path

        if stderr_path != None:
            spec.stderr_path = stderr_path

        if pre_launch != None:
            spec.pre_launch = pre_launch

        if post_launch != None:
            spec.post_launch = post_launch

        if launcher != None:
            spec.launcher = launcher

        if node_count != None:
            spec.resource.node_count = node_count

        if process_count != None:
            spec.resource.process_count = process_count

        if processes_per_node != None:
            spec.resource.processes_per_node = processes_per_node

        if cpu_cores_per_process != None:
            spec.resource.cpu_cores_per_process = cpu_cores_per_process

        if gpu_cores_per_process != None:
            spec.resource.gpu_cores_per_process = gpu_cores_per_process

        if exclusive_node_use:
            spec.resource.exclusive_node_use = exclusive_node_use

        if memory != None:
            spec.resource.memory = memory

        if duration != None:
            spec.attributes.duration = duration

        if queue_name != None:
            spec.attributes.queue_name = queue_name

        if account != None:
            spec.attributes.account = account

        if reservation_id != None:
            spec.attributes.reservation_id = reservation_id

        if custom_attributes != None:
            spec.attributes.custom_attributes = custom_attributes

        # spec.attributes.custom_attributes = {'node_shape.node': '1',
        #     'group.a':'ra010014',
        #     'pjsub_others.a':'-j',
        #     'pjsub_env.PJM_LLIO_GFSCACHE':'/vol0003:/vol0004:/vol0002'}

        return spec

    def submit( 
        self, 
        executable: str, 
        arguments: List[str], 
        job_spec: Dict[str, object] 
    ) -> psij.Job:
        job = psij.Job()
        job.spec = self.config_spec( executable = executable, arguments = arguments, **job_spec )
        self.job_executor.submit( job )
        return job

    def submit_python( 
        self, 
        func_obj, 
        executable: str = 'python',
        job_spec: Dict[str, object], 
        args: list[object] = [], 
        kwargs: Dict[str, object] = {},
        worker_mount_directory: Optional[str] = None
    ) -> psij.Job:

        work_directory = self.work_directory

        filename = f'{work_directory}/{job.id}.pkl'
        execute_path = f'{work_directory}/{job.id}.py'
        func_obj_path = f'{work_directory}/{job.id}.pkl'
        result_path = f'{work_directory}/{job.id}_out.pkl'
        if worker_mount_directory is not None:
            filename = f'{worker_mount_directory}/{job.id}.pkl'
            execute_path = f'{worker_mount_directory}/{job.id}.py'
            func_obj_path = f'{worker_mount_directory}/{job.id}.pkl'
            result_path = f'{worker_mount_directory}/{job.id}_out.pkl'
        # if( self.job_ex.name == 'pjsub' ):
        #     func_obj_path = f'/vol0003/mdt0{func_obj_path}'
        #     result_path = f'/vol0003/mdt0{result_path}'

        job = psij.Job()
        job.spec = self.config_spec( executable = executable, arguments = [ execute_path ], **job_spec )
        # job.spec = self.config_spec( work_directory, [ f'{work_directory}/{job.id}.py' ], **job_spec )

        # job = self.make_job( work_directory )

        if Path( execute_path ).is_file() or Path( func_obj_path ).is_file:
            Path( execute_path ).unlink( missing_ok=True )
            Path( func_obj_path ).unlink( missing_ok=True )

        self.python_serialize_function_and_args( filename, func_obj, args = args, kwargs = kwargs )

        with open( execute_path, 'w' ) as f:
            f.write( self.python_execute_script( func_obj_path, result_path ) )

        self.job_ex.submit( job )

        return job

    def wait_for_results( self, job ):
        status = job.wait()
        print( status )
        results = self.load_results( job.id )
        return results

    def load_results( self, job_id ):
        with open( f'{self.work_directory}/{job_id}_out.pkl', 'rb' ) as f:
            output = pickle.load( f )
        return { 'results': output[0], 'errers': output[1] }

