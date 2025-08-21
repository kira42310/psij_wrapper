import cloudpickle
import pickle
import psij

from pathlib import Path
from datetime import timedelta
from typing import Optional, Union, Dict

class psij_ext:

    # The Init function for the PSI/J wrapper, get the PSI/J instance and set up job executor parameters
    # Parameters:
    # - instance: str, The name of the job scheduler
    # - work_directory, Path, Optional, The location that job submission files write
    # - keep_files, bool, Optional, The parameter to keep the files after the job is finished
    # Return: No return
    def __init__( self, instance, work_directory: Optional[Path] = None, keep_files: Optional[bool] = False ):
        self.job_executor = psij.JobExecutor.get_instance( instance )
        self.job_executor.config.keep_files = keep_files

        # ToDo: Need to check the work_directory exist before using it, what if the PSI/J already doing it?
        if work_directory is not None:
            self.job_executor.work_directory = work_directory
        self.work_directory = self.job_executor.work_directory

    # Serialize the Python function and parameters to file
    # parameters:
    # - filename, Path, path and filename for the serialize python data
    # - func_obj, obj, Python function object
    # - args, list, Optional, Parameters for the python function object(For non key-value type)
    # - kwargs, dict, Optional, Parameters for the python function object(For key-value type)
    # Return: No return
    def python_serialize_function_and_args( self, filename: Path, func_obj, args: list = [], kwargs: Dict = {} ) -> None:
        with open( filename, 'wb' ) as obj_file:
            cloudpickle.dump( ( func_obj, args, kwargs ), obj_file )

    # Getter function for running Python serialize function
    # parameters:
    # - func_obj_path, str, Python serialize data file for execute in this raw source codee
    # - result_path, str, Output location
    # Return: str, python raw source code
    def python_execute_script( self, func_obj_path: str, result_path: str ) -> str:
        return f"""
import sys

sys.dont_write_bytecode = True

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

    # This function will create the PSI/J specification object and set the job schedulor parameters
    # parameters:
    # - executable, str, execution/binary/command
    # - Others optional parameters, please read at the PSI/J documentation
    # Return: PSI/J job specification object
    def config_spec( 
        self, 
        # work_directory,
        executable: str,
        arguments: list[str] = None,
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

        spec.executable = executable

        if arguments != None:
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

        return spec

    # This function will submit the job to the job scheduler
    # parameters:
    # - executation, str, executable/binary/command
    # - job_spec, Dict[ key, value ], Parameters for configure job specification
    # Return: PSI/J job object
    def submit( 
        self, 
        executable: str, 
        job_spec: Dict[str, object] 
    ) -> psij.Job:
        job = psij.Job()
        job.spec = self.config_spec( **job_spec )
        self.job_executor.submit( job )
        return job

    # This function is for submitting the serialize python function and execute with job scheduler
    # parameters:
    # - func_obj, object, Python function object
    # - job_spec, Dict[ key, value ], Parameters for configure job specification
    # - executable, str, executable/binary/command
    # - args, list, Optional, Parameters for the python function object(For non key-value type)
    # - kwargs, dict, Optional, Parameters for the python function object(For key-value type) 
    # - worker_mount_directory, Path, Optinoal, In case NFS on login node and compute node have different mounting point
    # Reutrn: PSI/J job object
    def submit_python( 
        self, 
        func_obj, 
        job_spec: Dict[str, object], 
        executable: str = 'python',
        args: list[object] = [], 
        kwargs: Dict[str, object] = {},
        worker_mount_directory: Union[str, Path, None] = None 
    ) -> psij.Job:

        work_directory = self.work_directory

        job = psij.Job()

        filename = f'{work_directory}/{job.id}.pkl'
        execute_path = f'{work_directory}/{job.id}.py'
        func_obj_path = f'{work_directory}/{job.id}.pkl'
        result_path = f'{work_directory}/{job.id}_out.pkl'
        if worker_mount_directory is not None:
            filename = f'{worker_mount_directory}/{job.id}.pkl'
            execute_path = f'{worker_mount_directory}/{job.id}.py'
            func_obj_path = f'{worker_mount_directory}/{job.id}.pkl'
            result_path = f'{worker_mount_directory}/{job.id}_out.pkl'

        job.spec = self.config_spec( arguments = [ execute_path ], **job_spec )

        if Path( execute_path ).is_file() or Path( func_obj_path ).is_file:
            Path( execute_path ).unlink( missing_ok=True )
            Path( func_obj_path ).unlink( missing_ok=True )

        self.python_serialize_function_and_args( filename, func_obj, args = args, kwargs = kwargs )

        with open( execute_path, 'w' ) as f:
            f.write( self.python_execute_script( func_obj_path, result_path ) )

        self.job_executor.submit( job )

        return job

    # Wait and read the result after the job is finished
    # parameters:
    # - job, PSI/J job object, PSI/J job object that need a results
    # Return: results value(Any)
    def wait_for_results( self, job ):
        status = job.wait()
        results = self.load_results( job.id )
        return results

    # Read the result from file
    # parameters:
    # - job_id, str, Job ID
    # Return: dict[ results, error ]
    def load_results( self, job_id ):
        with open( f'{self.work_directory}/{job_id}_out.pkl', 'rb' ) as f:
            output = pickle.load( f )
        return { 'results': output[0], 'errers': output[1] }

