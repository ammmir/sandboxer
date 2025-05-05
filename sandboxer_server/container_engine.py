import asyncio
import json
from typing import AsyncIterator, List, Optional, Dict, Tuple, Union, Callable, Awaitable
import tarfile
import tempfile
import os
import shutil
import subprocess
from ulid import ULID
import io
import re
import time
import aiofiles
import logging

from uvicorn.config import LOGGING_CONFIG
from uvicorn.logging import DefaultFormatter

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("uvicorn.error")

from .quota_manager import QuotaManager
from .subordinate_manager import SubordinateManager

class ContainerConfig:
    def __init__(self, cpus: str = "0.5", memory: str = "512m", storage_size: str = "5g"):
        self.capabilities = os.getenv("DEFAULT_CAPABILITIES",
            "NET_BIND_SERVICE,CHOWN,IPC_LOCK,SETGID,SETUID,SETGID,SETPCAP,SYS_CHROOT,SYS_NICE,SYS_PTRACE,DAC_READ_SEARCH,FOWNER,DAC_OVERRIDE,KILL"
        ).split(',')
        self.cpus = cpus
        self.memory = memory
        self.pids_limit = os.getenv("DEFAULT_PIDS_LIMIT", "1024")
        self.storage_size = storage_size

class Container:
    def __init__(self, engine: 'ContainerEngine', **kwargs):
        self.engine = engine
        self.id = kwargs.get("id", "unknown")
        self.ip_address = kwargs.get("ip_address", None)
        self.exposed_port = kwargs.get("exposed_port", None)
        self.hostname = kwargs.get("hostname", None)
        self.name = kwargs.get("name", "unknown")
        self.status = kwargs.get("status", "unknown")
        self.image = kwargs.get("image", "unknown")
        self.image_id = kwargs.get("image_id", "unknown")
        self.created_at = kwargs.get("created_at", -1)
        self.started_at = kwargs.get("started_at", -1)
        self.exited_at = kwargs.get("exited_at", -1)
        self.command = kwargs.get("command", [])
        self.interactive = kwargs.get("interactive", False)

    async def exec(self, command: Union[str, List[str]], stream: bool = False) -> Union[Tuple[str, str, int], AsyncIterator[Tuple[str, str, int]]]:
        return await self.engine.exec(self.id, command, stream=stream)

    async def logs(self, stream: bool = False, cancel_event: asyncio.Event = None) -> Union[str, AsyncIterator[str]]:
        return await self.engine.logs(self.id, stream=stream)

    async def stop(self) -> None:
        await self.engine.stop_container(self.id)

    async def snapshot(self, image_path: str) -> None:
        await self.engine.snapshot_container(self.id, image_path)

    async def pause(self) -> None:
        await self.engine.pause_sandbox(self.id)

    async def unpause(self) -> None:
        await self.engine.unpause_sandbox(self.id)

class ContainerEngine:
    PODMAN_PATH = 'podman'
    USE_UIDMAP = False

    def __init__(self, quota: QuotaManager, podman_path: str = PODMAN_PATH):
        self.quota = quota
        self.podman_path = podman_path.split()
        self.inspect_cache = {}
        self._events_task = None
        self._cache_lock = asyncio.Lock()
        self._subscribers = set()
        self._shutdown_event = asyncio.Event()
        info = subprocess.run([ContainerEngine.PODMAN_PATH, 'info', '-f', '{{.Store.GraphRoot}},{{.Store.RunRoot}}'], capture_output=True, text=True)
        self.podman_graphroot, self.podman_runroot = info.stdout.strip().split(',')
        logger.info(f"Podman GraphRoot: {self.podman_graphroot} RunRoot: {self.podman_runroot}")

    async def subscribe(self) -> asyncio.Queue:
        """Subscribe to container events. Returns a queue that will receive events."""
        if self._shutdown_event.is_set():
            raise RuntimeError("ContainerEngine is shutting down")
        queue = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Unsubscribe from container events."""
        self._subscribers.discard(queue)

    async def _emit_event(self, event_type: str, container_id: str, data: dict = None) -> None:
        """Emit an event to all subscribers."""
        if self._shutdown_event.is_set():
            return
        event = {
            "type": event_type,
            "container_id": container_id,
            "timestamp": time.time(),
            "data": data or {}
        }
        for queue in self._subscribers:
            try:
                await queue.put(event)
            except asyncio.CancelledError:
                # If queue is closed, remove it from subscribers
                self._subscribers.discard(queue)

    async def _monitor_events(self):
        """Background task to monitor podman events and update inspect cache."""
        args = self.podman_path + ['system', 'events', '--stream', '--format', 'json']
        
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        try:
            while not self._shutdown_event.is_set():
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=1.0)
                    if not line:  # EOF
                        break
                    
                    try:
                        event = json.loads(line.decode())
                        if event['Type'] == 'container':
                            container_id = event['ID']
                            logger.debug(f"Container {container_id} {event['Status']}")

                            # Update cache
                            inspect_output = await self._run_podman_command(['inspect', container_id])
                            async with self._cache_lock:
                                self.inspect_cache[container_id] = inspect_output

                            if event['Status'] == 'start':
                                await self._emit_event("container_started", container_id)
                            elif event['Status'] == 'died':
                                await self._emit_event("container_stopped", container_id)
                            elif event['Status'] == 'pause':
                                await self._emit_event("container_paused", container_id)
                            elif event['Status'] == 'unpause':
                                await self._emit_event("container_unpaused", container_id)
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"Error processing podman event: {e}")
                        continue
                except asyncio.TimeoutError:
                    # Check shutdown event periodically
                    continue
        finally:
            try:
                process.terminate()
                await process.wait()
            except ProcessLookupError:
                pass

    async def start(self):
        """Start the event monitoring task."""
        self._events_task = asyncio.create_task(self._monitor_events())

    async def close(self):
        """Clean up resources."""
        # Signal shutdown
        self._shutdown_event.set()
        
        # Cancel event monitoring task
        if self._events_task:
            self._events_task.cancel()
            try:
                await self._events_task
            except asyncio.CancelledError:
                pass
            self._events_task = None

        # Close all subscriber queues
        for queue in self._subscribers:
            try:
                await queue.put(None)  # Signal end of events
            except asyncio.CancelledError:
                pass
        self._subscribers.clear()

    async def _get_container_info(self, container_id: str) -> dict:
        """Get container info from cache or by running inspect."""
        async with self._cache_lock:
            if container_id in self.inspect_cache:
                return json.loads(self.inspect_cache[container_id])[0]
        
        # If not in cache, run inspect and cache the result
        output = await self._run_podman_command(['inspect', container_id])
        async with self._cache_lock:
            self.inspect_cache[container_id] = output
        return json.loads(output)[0]

    async def _run_podman_command(self, args: List[str]) -> str:
        """Run a Podman CLI command asynchronously and return its output."""
        command = self.podman_path + ['--log-level=debug'] + args
        print(f'Executing: {" ".join(command)}')

        def run_in_thread():
            process = subprocess.run(command, capture_output=True, text=True)
            # Log stdout and stderr
            #if process.stdout:
            #    print(f'stdout: {process.stdout}')
            #if process.stderr:
            #    print(f'stderr: {process.stderr}')
            return process

        try:
            loop = asyncio.get_running_loop()
            process = await loop.run_in_executor(None, run_in_thread)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Podman command timed out: {' '.join(command)}")

        if process.returncode != 0:
            raise RuntimeError(f"Podman command failed: {process.stderr.strip()}")

        return process.stdout.strip()
 
    async def list_containers(self, filter_label: Optional[str] = None) -> List[Container]:
        """List all containers, optionally filtering by label."""
        args = ['container', 'list', '-a', '--format', 'json']
        if filter_label:
            args.extend(['-f', f'label={filter_label}'])

        output = await self._run_podman_command(args)
        containers_data = json.loads(output)

        containers = []
        for data in containers_data:
            ip_address, exposed_port = await self._get_container_ip(data["Id"])
            containers.append(
                Container(
                    id=data["Id"],
                    hostname=data["Config"].get("Hostname", None),
                    ip_address=ip_address,
                    exposed_port=exposed_port,
                    engine=self,
                    name=data["Names"][0] if data["Names"] else "unknown",
                    status=data["State"].lower(),  # Convert to lowercase for consistency
                    image=data["Image"],
                    image_id=data["ImageID"],
                    created_at=data["Created"],
                    started_at=data["StartedAt"],
                    exited_at=data["ExitedAt"],
                    command=data["Command"]
                )
            )
        return containers

    async def exec(self, container_id: str, command: Union[str, List[str]], stream: bool = False) -> Union[Tuple[str, str, int], AsyncIterator[Tuple[str, str, int]]]:
        """
        Execute a command inside the container.

        - `stream=False` (default): Returns `(stdout, stderr, exit_code)`.
        - `stream=True`: Returns an async iterator yielding tuples `(stream_type, data, exit_code)`.
        """
        args = self.podman_path + ['exec', '-i', container_id]

        if isinstance(command, str):
            # If it's a single command with spaces, wrap it in `sh -c`
            if " " in command:
                args.extend(['sh', '-c', command])
            else:
                args.append(command)
        else:
            args.extend(command)  # Use the provided list as-is

        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        if stream:
            async def output_stream() -> AsyncIterator[Tuple[str, str, int]]:
                """Stream output line by line with an exit code of -1 until the process exits."""
                exit_code = -1
                queue = asyncio.Queue()
                done = asyncio.Event()

                async def read_stream(stream: Optional[asyncio.StreamReader], stream_type: str):
                    if stream:
                        while True:
                            chunk = await stream.read(1024)
                            if not chunk:
                                break
                            await queue.put((stream_type, chunk.decode().strip(), exit_code))
                    done.set()

                # Start reading from both streams
                read_task = asyncio.gather(
                    read_stream(process.stdout, "stdout"),
                    read_stream(process.stderr, "stderr")
                )

                try:
                    while not done.is_set() or not queue.empty():
                        try:
                            # Wait for data with a small timeout to check done event
                            yield await asyncio.wait_for(queue.get(), timeout=0.1)
                        except asyncio.TimeoutError:
                            continue
                finally:
                    read_task.cancel()
                    try:
                        await read_task
                    except asyncio.CancelledError:
                        pass

                exit_code = await process.wait()  # Wait for the process to exit
                yield "exit", f"Process exited with code {exit_code}", exit_code

            return output_stream()

        # Default: Capture full output and return
        stdout, stderr = await process.communicate()
        exit_code = process.returncode

        return stdout.decode().strip(), stderr.decode().strip(), exit_code

    async def logs(self, container_id: str, stream: bool = False, cancel_event: asyncio.Event = None) -> Union[str, AsyncIterator[str]]:
        """
        Get container logs.

        - `stream=False` (default): Returns all logs as a single string.
        - `stream=True`: Returns an async iterator yielding log lines as they come.
        - `cancel_event`: Optional event that when set will stop the streaming.
        """
        args = self.podman_path + ['logs']
        if stream:
            args.append('-f')
        args.append(container_id)

        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT  # Merge stderr into stdout for container logs
        )

        if stream:
            async def output_stream() -> AsyncIterator[str]:
                """Stream log lines as they come."""
                try:
                    if process.stdout:
                        while True:
                            if cancel_event and cancel_event.is_set():
                                break
                            try:
                                line = await asyncio.wait_for(process.stdout.readline(), timeout=0.1)
                                if not line:  # EOF
                                    break
                                yield line.decode().strip()
                            except asyncio.TimeoutError:
                                # Check cancellation more frequently
                                continue
                finally:
                    # Ensure process is terminated and cleaned up
                    try:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    except (asyncio.TimeoutError, ProcessLookupError):
                        if process.returncode is None:
                            process.kill()  # Force kill if terminate didn't work
                            try:
                                await process.wait()
                            except ProcessLookupError:
                                pass  # Process already gone

            return output_stream()

        # Default: Capture all logs and return as a single string
        stdout, _ = await process.communicate()
        return stdout.decode().strip()

    async def create_network(self, name: str) -> str:
        """Create a new Podman network and return its name."""
        await self._run_podman_command(['network', 'create', '-o', 'isolate=true', name])
        return name

    async def remove_network(self, name: str) -> None:
        """Remove a Podman network by name."""
        await self._run_podman_command(['network', 'remove', name])

    async def inspect_network(self, name: str) -> dict:
        """Inspect a Podman network and return its details."""
        output = await self._run_podman_command(['network', 'inspect', name])
        return json.loads(output)[0]

    async def start_container(self, image: str, name: str, subuid: int, quota_projname: str, config: 'ContainerConfig', env: dict[str, str] = None, args: list[str] = None, network: str = None, interactive: bool = False, hostname: str = None) -> Container:
        """
        Start a new container using `podman create` and `podman start`.
        Returns a `Container` object with its assigned IP address.
        
        Args:
            image: Container image to use
            name: Container name
            quota_projname: Project name to use for quota management
            config: Container configuration object
            env: Environment variables
            args: Additional container arguments
            network: Network to connect to
            interactive: Whether to run in interactive mode
        """
        # First create the container
        create_args = [
            'create',
            '--cap-drop=ALL',
            '--security-opt', 'no-new-privileges',
            '--cpus', config.cpus,
            '--memory', config.memory,
            '--image-volume', 'ignore',
            '--pids-limit', config.pids_limit,
            '--name', name,  # Container name
            '--label', f'quota_projname={quota_projname}',  # Store quota project name in labels
        ]

        if hostname:
            create_args.extend(['--hostname', hostname])

        if ContainerEngine.USE_UIDMAP:
            create_args.extend(['--uidmap', f'0:{subuid}:{SubordinateManager.BLOCK_SIZE}'])

        # Add storage-opt only if quota is supported
        if self.quota.is_supported():
            create_args.extend(['--storage-opt', f'size={config.storage_size}'])

        # Add capabilities from config
        for cap in config.capabilities:
            if cap.strip():  # Skip empty strings
                create_args.extend(['--cap-add', cap.strip()])

        # Add interactive mode if requested
        if interactive:
            create_args.extend(['-it'])  # Add -it for interactive mode

        # Add network if specified
        if network:
            create_args.extend(['--network', network])

        # Add environment variables if provided
        if env:
            for key, value in env.items():
                create_args.extend(['-e', f'{key}={value}'])

        create_args.append(f'docker://{image}')  # The image to run

        # Add additional container arguments if provided
        if args:
            create_args.extend(args)
        
        container_id = await self._run_podman_command(create_args)

        # Start the container
        await self._run_podman_command(['start', container_id])

        container_ip, exposed_port = await self._get_container_ip(container_id)

        return Container(
            engine=self,
            id=container_id,
            ip_address=container_ip,
            exposed_port=exposed_port,
            hostname=hostname,
            name=name,
            image=image
        )

    async def _get_listening_ports(self, pid: int) -> List[int]:
        try:
            async with aiofiles.open(f"/proc/{pid}/net/tcp", "r") as f:
                lines = await f.readlines()
                
            ports = []
            for line in lines[1:]:  # Skip header line
                fields = line.strip().split()
                if len(fields) >= 4 and fields[3] == "0A":  # 0A is listening state
                    # Parse local_address:port (hex)
                    local_addr = fields[1].split(":")
                    if len(local_addr) == 2:
                        port_hex = local_addr[1]
                        port = int(port_hex, 16)
                        ports.append(port)
            
            # Sort ports with common web server ports first
            common_ports = [80, 8000, 8080, 3000, 4000]
            ports.sort(key=lambda x: (common_ports.index(x) if x in common_ports else len(common_ports), x))
            
            return ports
        except (FileNotFoundError, PermissionError, ValueError):
            return []

    async def _get_container_ip(self, container_id: str) -> Tuple[str, Optional[str]]:
        """Inspect a container and return its assigned private IP and first exposed port."""
        container_info = await self._get_container_info(container_id)

        # First try the default network IP address
        ip_address = container_info["NetworkSettings"]["IPAddress"]
        
        # If IP is empty and there are networks, get the first network's IP
        if not ip_address and container_info["NetworkSettings"]["Networks"]:
            # Get the first network's IP address
            first_network = next(iter(container_info["NetworkSettings"]["Networks"].values()))
            ip_address = first_network.get("IPAddress", "")
        
        # Extract the first exposed port (even if its value is null)
        ports = container_info["NetworkSettings"]["Ports"]
        first_exposed_port = next(iter(ports.keys()), None) if ports else None
        first_exposed_port = first_exposed_port.split('/')[0] if first_exposed_port else None

        # If no exposed port found, try to get listening ports from the container
        if first_exposed_port is None:
            try:
                # Get container's PID
                pid = container_info["State"]["Pid"]
                if pid > 0:  # Only if container is running
                    listening_ports = await self._get_listening_ports(pid)
                    if listening_ports:
                        first_exposed_port = str(listening_ports[0])
            except (KeyError, ValueError):
                pass

        return ip_address, first_exposed_port

    async def get_container(self, container_id: str) -> Optional[Container]:
        """Inspect a container and return a `Container` object."""
        try:
            data = await self._get_container_info(container_id)
            ip_address, first_exposed_port = await self._get_container_ip(container_id)

            # Check if container is interactive by looking at the TTY flag
            interactive = data["Config"].get("Tty", False)

            hostname = data["Config"].get("Hostname", None)

            return Container(
                engine=self,
                id=data["Id"],
                ip_address=ip_address,
                exposed_port=first_exposed_port,
                hostname=hostname,
                name=data.get("Name", "").lstrip("/"),
                status=data["State"]["Status"].lower(),  # Get status from State.Status
                image=data.get("Image", "unknown"),
                image_id=data.get("ImageID", "unknown"),
                created_at=data.get("Created", -1),
                started_at=data["State"].get("StartedAt", -1),
                exited_at=data["State"].get("ExitedAt", -1),
                command=data["Config"].get("Cmd", []),
                interactive=interactive
            )
        except RuntimeError:
            return None  # If the container doesn't exist

    async def stop_container(self, container_id: str) -> None:
        # Stop the container
        #await self._run_podman_command(['stop', '--time', '3', container_id])
        await self._run_podman_command(['kill', '-s', 'KILL', container_id])

    async def remove_container(self, container_id: str) -> None:
        await self._run_podman_command(['rm', container_id])

    async def snapshot_container(self, container_id: str, image_path: str) -> None:
        await self._run_podman_command([
            'container', 
            'checkpoint', 
            '-e', image_path, 
            '--leave-running', 
            '--ignore-volumes', 
            '--tcp-established', 
            '--tcp-skip-in-flight', 
            '--ext-unix-sk', 
            container_id
        ])

    async def restore_container(self, image_path: str, name: str, quota_projname: str) -> Container:
        """
        Restore a container from a checkpoint image.
        Returns a `Container` object with its assigned IP address.
        
        Args:
            image_path: Path to the checkpoint image
            name: Name for the restored container
            quota_projname: Project name to use for quota management
        """
        # Restore the container
        args = [
            'container', 'restore',
            '-i', image_path,
            '-n', name,
            '--ignore-volumes',
            '--label', f'quota_projname={quota_projname}'  # Store quota project name in labels
        ]
        container_id = await self._run_podman_command(args)

        # Get the container's UpperDir
        container_info = await self._get_container_info(container_id)
        upper_dir = container_info["GraphDriver"]["Data"]["UpperDir"]

        container_ip = await self._get_container_ip(container_id)

        return Container(
            engine=self,
            id=container_id,
            ip_address=container_ip,
            hostname=hostname,
            name=name
        )

    async def _get_container_volumes(self, container_id: str) -> Dict[str, str]:
        """Find named volumes attached to a container."""
        container_info = await self._get_container_info(container_id)
        volume_map = {}
        for mount in container_info.get("Mounts", []):
            if mount["Type"] == "volume":
                volume_map[mount["Destination"]] = mount["Name"]
        return volume_map

    async def _duplicate_volumes(self, container_id: str, volume_map: Dict[str, str]) -> Dict[str, str]:
        """Duplicate container volumes by creating new ones and copying data."""
        new_volumes = {}

        for mount_path, old_volume in volume_map.items():
            new_volume = f'{old_volume}_fork_{str(ULID()).lower()}'

            # Create the new volume
            await self._run_podman_command(['volume', 'create', '--label', f'sbx_id={container_id}', new_volume])

            # Copy the data using export/import
            export_cmd = [self.podman_path, "volume", "export", old_volume]
            import_cmd = [self.podman_path, "volume", "import", new_volume, "-"]

            with subprocess.Popen(export_cmd, stdout=subprocess.PIPE) as export_proc, \
                 subprocess.Popen(import_cmd, stdin=export_proc.stdout) as import_proc:
                export_proc.stdout.close()
                import_proc.wait()

            new_volumes[mount_path] = new_volume

        return new_volumes

    async def _edit_checkpoint(self, snapshot_file: str, volume_map: Dict[str, str], new_volumes: Dict[str, str], container_uid: int, container_gid: int, hostname: str = None) -> None:
        """Modify the checkpoint tar file by creating a new one with updated volume references."""
        temp_output = f"{snapshot_file}.tmp"
        if not hostname:
            hostname = f"{str(ULID()).lower()}"
        
        try:
            with tarfile.open(snapshot_file, 'r:gz') as input_archive:
                with tarfile.open(temp_output, 'w:gz') as output_archive:
                    for member in input_archive.getmembers():
                        file = input_archive.extractfile(member)
                        
                        # If this is one of our target files, modify its contents
                        if member.name in ["config.dump", "spec.dump"]:
                            content = file.read().decode('utf-8')
                            
                            # Replace volume references
                            for mount_path, old_volume in volume_map.items():
                                new_volume = new_volumes[mount_path]
                                content = content.replace(old_volume, new_volume)
                                content = content.replace(
                                    f"/containers/storage/volumes/{old_volume}/_data",
                                    f"/containers/storage/volumes/{new_volume}/_data"
                                )

                            # Replace hostname in config.dump (JSON)
                            try:
                                config = json.loads(content)

                                if "spec" in config and "process" in config["spec"]:
                                    config["spec"]["hostname"] = hostname
                                    # replace the HOSTNAME env var in process.env
                                    for i, env in enumerate(config["spec"]["process"]["env"]):
                                        if "HOSTNAME" in env:
                                            config["spec"]["process"]["env"][i] = f"HOSTNAME={hostname}"
                                            break

                                if "hostname" in config:
                                    config["hostname"] = hostname

                                # also need to update newNetworks.<the network name>.aliases array
                                for network in config.get("newNetworks", {}).values():
                                    if "aliases" in network:
                                        network["aliases"] = [hostname]

                                content = json.dumps(config, indent=2)
                            except Exception as e:
                                logger.error(f"Error replacing hostname in config.dump: {e}")
                            
                            # Create a new tarinfo with the same metadata but updated size
                            new_content = content.encode('utf-8')
                            new_info = tarfile.TarInfo(member.name)
                            new_info.size = len(new_content)
                            new_info.mode = member.mode
                            new_info.uid = container_uid
                            new_info.gid = container_gid
                            new_info.mtime = member.mtime
                            logger.info(f"Adding modified file {member.name} with UID {container_uid} and GID {container_gid}")

                            # Add modified content to archive
                            output_archive.addfile(new_info, fileobj=io.BytesIO(new_content))
                        elif member.name == "checkpoint/utsns-12.img":
                            content = file.read()
                            
                            # Create a subprocess to decode the UTS namespace
                            decode_process = await asyncio.create_subprocess_exec(
                                'crit', 'decode',
                                stdin=asyncio.subprocess.PIPE,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            
                            # Send the content to crit decode
                            stdout, stderr = await decode_process.communicate(input=content)
                            if decode_process.returncode != 0:
                                raise RuntimeError(f"crit decode failed: {stderr.decode()}")
                            
                            # Parse the JSON output
                            uts_data = json.loads(stdout)
                            
                            # Update the nodename with the new hostname
                            uts_data["entries"][0]["nodename"] = hostname
                            
                            # Create a subprocess to encode the updated UTS namespace
                            encode_process = await asyncio.create_subprocess_exec(
                                'crit', 'encode',
                                stdin=asyncio.subprocess.PIPE,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )

                            logger.info(f'Updating UTS namespace: {json.dumps(uts_data)}')
                            
                            # Send the updated JSON to crit encode
                            stdout, stderr = await encode_process.communicate(input=json.dumps(uts_data).encode())
                            if encode_process.returncode != 0:
                                raise RuntimeError(f"crit encode failed: {stderr.decode()}")
                            
                            # Create a new TarInfo object with the updated content
                            new_content = stdout
                            new_info = tarfile.TarInfo(member.name)
                            new_info.size = len(new_content)
                            new_info.mode = member.mode
                            new_info.uid = container_uid
                            new_info.gid = container_gid
                            new_info.mtime = member.mtime
                            logger.info(f"Adding modified file {member.name} with UID {container_uid} and GID {container_gid}")

                            # Add modified content to archive
                            output_archive.addfile(new_info, fileobj=io.BytesIO(new_content))
                        else:
                            # For all other files, copy them with updated UID/GID
                            member.uid = container_uid
                            member.gid = container_gid
                            logger.info(f"Adding file {member.name} with UID {container_uid} and GID {container_gid}")
                            output_archive.addfile(member, file)
            
            # Replace the original file with our modified version
            os.replace(temp_output, snapshot_file)
            
        except Exception as e:
            # Clean up temp file if something goes wrong
            if os.path.exists(temp_output):
                os.unlink(temp_output)
            raise e
        
        return snapshot_file

    async def _copy_and_modify_container_files(self, parent_id: str, new_id: str, container_uid: int, container_gid: int, hostname: str = None) -> None:
        """
        Copy and modify container-specific files from parent to new container.
        This includes /etc/hosts, /etc/resolv.conf, and /etc/hostname.
        """

        # Get the userdata paths for both containers
        parent_path = f"{self.podman_runroot}/overlay-containers/{parent_id}/userdata"
        new_path = f"{self.podman_runroot}/overlay-containers/{new_id}/userdata"

        # Get the new container's hostname and IPs
        new_ip, _ = await self._get_container_ip(new_id)
        parent_ip, _ = await self._get_container_ip(parent_id)

        # Copy and modify hosts file
        with open(f"{parent_path}/hosts", 'r') as f:
            hosts_content = f.read()

        # Replace the parent container's hostname
        hosts_content = hosts_content.replace(parent_id[:12], new_id[:12])

        # Replace parent IP with new IP (equivalent to s/{parent_ip}\s+/{new_ip} /g)
        hosts_content = re.sub(f'{parent_ip}\\s+', f'{new_ip}\t', hosts_content)

        with open(f"{new_path}/hosts", 'w') as f:
            f.write(hosts_content)

        # Set UID/GID for hosts file
        os.chown(f"{new_path}/hosts", container_uid, container_gid)

        # Copy resolv.conf as is
        shutil.copy2(f"{parent_path}/resolv.conf", f"{new_path}/resolv.conf")

        # Set UID/GID for resolv.conf file
        os.chown(f"{new_path}/resolv.conf", container_uid, container_gid)

        # TODO: hostname needs to be changed at the UTS level or in the CRIU snapshot
        # XXX: running programs may already have called gethostname() so it's pointless
        # XXX: Podman's container state will be out of sync

        # Copy and modify hostname file
        with open(f"{new_path}/hostname", 'w') as f:
            logger.info(f"Writing new hostname to {new_path}/hostname: {hostname}")
            f.write(hostname)

        # Set UID/GID for hostname file
        os.chown(f"{new_path}/hostname", container_uid, container_gid)

        # Modify graphroot/overlay-containers/<new id>/config.json with the new hostname
        with open(f"{self.podman_graphroot}/overlay-containers/{new_id}/userdata/config.json", 'r') as f:
            config = json.load(f)

        config["hostname"] = hostname

        with open(f"{self.podman_graphroot}/overlay-containers/{new_id}/userdata/config.json", 'w') as f:
            json.dump(config, f)

    async def fork_container(self, container_id: str, new_name: str, keep_snapshot: bool = False, hostname: str = None) -> Tuple[Container, str]:
        """
        Forks a running container by:
        1. Creating a snapshot
        2. Duplicating volumes
        3. Editing the checkpoint metadata
        4. Restoring the container with new volumes
        5. Copying and modifying container-specific files

        Returns:
        - A tuple containing:
        - `Container`: The new forked container.
        - `str`: Path to the modified snapshot file.

        If `keep_snapshot=True`, the snapshot file will be retained after forking.
        """
        # Get the original container's quota project name
        container_info = await self._get_container_info(container_id)
        quota_projname = container_info["Config"]["Labels"].get("quota_projname")
        if not quota_projname:
            raise RuntimeError("Original container has no quota project name")

        if not hostname:
            hostname = f"{container_info['Config']['Hostname']}-{str(ULID()).lower()[:8]}"

        # Get the mapped UID/GID for root (0) inside the container
        if ContainerEngine.USE_UIDMAP:
            uid_map = container_info["HostConfig"]["IDMappings"]["UidMap"][0]
            gid_map = container_info["HostConfig"]["IDMappings"]["GidMap"][0]
            # Parse the mapping (format: "container_id:host_id:size")
            container_uid = int(uid_map.split(':')[1])
            container_gid = int(gid_map.split(':')[1])
        else:
            container_uid = 0
            container_gid = 0

        logger.info(f"Forking container {container_id} with UID {container_uid} and GID {container_gid}")

        snapshot_file = f"{new_name}.tar.gz"

        # Step 1: Get container volume mappings
        volume_map = await self._get_container_volumes(container_id)

        # Step 2: Create snapshot
        await self._run_podman_command(['container', 'checkpoint', '-c', 'gzip', '-e', snapshot_file, '--ignore-volumes', '--tcp-established', '--leave-running', container_id])

        # Step 3: Duplicate volumes
        new_volumes = await self._duplicate_volumes(container_id, volume_map)

        # Step 4: Modify the snapshot metadata
        modified_snapshot = await self._edit_checkpoint(snapshot_file, volume_map, new_volumes, container_uid, container_gid, hostname)

        # Step 5: Restore the modified checkpoint with the same quota project name
        restored_id = await self._run_podman_command([
            'container', 'restore',
            '-i', modified_snapshot,
            '--ignore-volumes',
            '-n', new_name
        ])

        container_ip = await self._get_container_ip(restored_id)

        # Step 6: Copy and modify container-specific files
        await self._copy_and_modify_container_files(container_id, restored_id, container_uid, container_gid, hostname)

        # Delete snapshot if requested
        if not keep_snapshot:
            try:
                os.remove(modified_snapshot)
            except OSError as e:
                print(f"Failed to delete snapshot {modified_snapshot}: {e}")

        return Container(id=restored_id, name=new_name, ip_address=container_ip, engine=self, hostname=hostname), modified_snapshot

    async def attach_container(self, container_id: str) -> Tuple[Callable[[], Awaitable[None]], Callable[[bytes], Awaitable[None]], AsyncIterator[bytes]]:
        """
        Attach to a running container's stdin/stdout/stderr.
        Returns a tuple containing:
        - A coroutine that when called will detach from the container and clean up resources
        - A coroutine for sending data to stdin
        - An async iterator for receiving data from stdout/stderr
        """
        #print(f"🔌 Attaching to container {container_id}")
        # Create the attach command
        args = self.podman_path + ['attach', container_id]
        #print(f"Running command: {' '.join(args)}")
        
        # Start the attach process
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT  # Merge stderr into stdout
        )
        #print(f"Process started with PID {process.pid}")

        # Create event for detaching
        detach_event = asyncio.Event()
        # Create a queue for output
        output_queue = asyncio.Queue()

        async def send_data(data: bytes) -> None:
            """Send data to the container's stdin."""
            if not detach_event.is_set():
                try:
                    #print(f"📤 Sending data: {data}")
                    process.stdin.write(data)
                    await process.stdin.drain()
                    #print("Data sent successfully")
                except Exception as e:
                    #print(f"❌ Error sending data: {e}")
                    detach_event.set()

        async def read_output():
            """Read output from stdout and put it in the queue."""
            try:
                while not detach_event.is_set():
                    try:
                        # Read in small chunks (1KB) to get data as soon as it's available
                        chunk = await asyncio.wait_for(process.stdout.read(1024), timeout=0.1)
                        if not chunk:  # EOF
                            print("EOF reached")
                            break
                        await output_queue.put(chunk)
                    except asyncio.TimeoutError:
                        # Check if we should continue waiting
                        continue
                    except asyncio.CancelledError:
                        print("Cancelled")
                        break
                    except Exception as e:
                        print(f"❌ Error reading from stdout: {e}")
                        break
            finally:
                if not detach_event.is_set():
                    await cancel()

        async def receive_data() -> AsyncIterator[bytes]:
            """Receive data from the queue."""
            #print("📥 Starting to receive data")
            try:
                while not detach_event.is_set():
                    try:
                        # Get data from queue with timeout
                        try:
                            chunk = await asyncio.wait_for(output_queue.get(), timeout=0.1)
                            yield chunk
                        except asyncio.TimeoutError:
                            # No data available, yield empty to allow other operations
                            yield b""
                            continue
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        print(f"❌ Error receiving data: {e}")
                        break
            finally:
                # Ensure the read_output task is cancelled
                if not detach_event.is_set():
                    detach_event.set()

        # Start the read_output task
        read_task = asyncio.create_task(read_output())

        async def cancel():
            """Cancel the attachment and clean up resources."""
            if True or not detach_event.is_set():
                detach_event.set()
                try:
                    # Send detach sequence
                    process.stdin.write(b'\x10\x11')
                    await process.stdin.drain()
                except Exception as e:
                    print(f"❌ Error sending detach sequence: {e}")
                
                # Terminate the process
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except (asyncio.TimeoutError, ProcessLookupError):
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                
                # Cancel the read task
                read_task.cancel()
                try:
                    await read_task
                except asyncio.CancelledError:
                    pass


        return cancel, send_data, receive_data

    async def stat(self, container_id: str, path: str) -> dict:
        """
        Get information about a file or directory in the container.
        Returns a dictionary with file/directory information.
        """
        container_info = await self._get_container_info(container_id)
        merged_dir = container_info["GraphDriver"]["Data"]["MergedDir"]
        
        # Normalize and sanitize the path
        path = os.path.normpath(path)
        if not path.startswith('/'):
            path = '/' + path  # Ensure path starts with /
        
        # Construct full path and ensure it's within the container
        full_path = os.path.join(merged_dir, path.lstrip('/'))
        if not full_path.startswith(merged_dir):
            raise RuntimeError("Path traversal not allowed")
        
        # Check if path exists
        if not os.path.exists(full_path):
            raise RuntimeError("Path not found")
        
        # Get file info
        try:
            stat = os.stat(full_path)
            is_dir = os.path.isdir(full_path)
            is_file = os.path.isfile(full_path)

            return {
                "path": path,
                "is_dir": is_dir,
                "is_file": is_file,
                "size": stat.st_size if is_file else 0,
                "modified": stat.st_mtime,
                "mode": stat.st_mode
            }
        except (OSError, PermissionError) as e:
            raise RuntimeError(f"Error getting file info: {str(e)}")

    async def list_files(self, container_id: str, path: str = '/') -> dict:
        """
        List files and directories in a container's filesystem.
        Returns a dictionary with 'entries' containing file/directory information.
        """
        # First check if path is a directory
        info = await self.stat(container_id, path)
        if not info["is_dir"]:
            raise RuntimeError("Path is not a directory")
        
        container_info = await self._get_container_info(container_id)
        merged_dir = container_info["GraphDriver"]["Data"]["MergedDir"]
        full_path = os.path.join(merged_dir, path.lstrip('/'))
        
        # List directory contents
        entries = []
        try:
            for entry in os.listdir(full_path):
                entry_path = os.path.join(full_path, entry)
                
                # Skip symlinks entirely
                if os.path.islink(entry_path):
                    continue
                    
                # Get entry info
                try:
                    is_dir = os.path.isdir(entry_path)
                    size = os.path.getsize(entry_path) if not is_dir else 0
                    
                    entries.append({
                        "name": entry,
                        "is_dir": is_dir,
                        "size": size,
                        "path": os.path.join(path, entry)
                    })
                    
                    # Break after collecting 1000 entries
                    if len(entries) >= 1000:
                        break
                        
                except (OSError, PermissionError):
                    # Skip entries we can't access
                    continue
                    
        except FileNotFoundError:
            raise RuntimeError("Directory not found")
        except PermissionError:
            raise RuntimeError("Permission denied")
        except Exception as e:
            raise RuntimeError(f"Error listing directory: {str(e)}")
        
        return {"entries": entries}

    async def get_file(self, container_id: str, path: str) -> tuple[str, str, str]:
        """
        Get the content of a file in the container.
        Returns a tuple of (content_type, filename, file_path).
        The actual file content should be streamed using the returned file path.
        """
        # First check if path is a file
        info = await self.stat(container_id, path)
        if not info["is_file"]:
            raise RuntimeError("Path is not a file")
        
        container_info = await self._get_container_info(container_id)
        merged_dir = container_info["GraphDriver"]["Data"]["MergedDir"]
        full_path = os.path.join(merged_dir, path.lstrip('/'))
        
        try:
            # Get filename and content type
            filename = os.path.basename(path)
            import mimetypes
            content_type, _ = mimetypes.guess_type(filename)
            if not content_type:
                content_type = 'application/octet-stream'
            
            # Return the file path and metadata
            return content_type, filename, full_path
            
        except PermissionError:
            raise RuntimeError("Permission denied")
        except Exception as e:
            raise RuntimeError(f"Error reading file: {str(e)}")

    async def put_file(self, container_id: str, path: str, file_obj) -> None:
        """
        Save a file to the container's filesystem by streaming from a file-like object.
        Sets file permissions to 644 and ownership to the mapped root user inside the container.
        """
        container_info = await self._get_container_info(container_id)
        merged_dir = container_info["GraphDriver"]["Data"]["MergedDir"]
        full_path = os.path.join(merged_dir, path.lstrip('/'))
        
        try:
            logger.info(f"Starting file upload to container {container_id}: {path}")
            
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            # Stream the file content to disk using async API
            async with aiofiles.open(full_path, 'wb') as f:
                while chunk := await file_obj.read(8192):  # Read in 8KB chunks
                    await f.write(chunk)
            
            # Close the file object
            await file_obj.close()
            
            # Set file permissions to 644 (rw-r--r--)
            os.chmod(full_path, 0o644)
            logger.info(f"Set file permissions to 644 for {path}")
            
            # Get the mapped UID/GID for root (0) inside the container
            if ContainerEngine.USE_UIDMAP:
                uid_map = container_info["HostConfig"]["IDMappings"]["UidMap"][0]
                gid_map = container_info["HostConfig"]["IDMappings"]["GidMap"][0]
                # Parse the mapping (format: "container_id:host_id:size")
                container_uid = int(uid_map.split(':')[1])
                container_gid = int(gid_map.split(':')[1])
            else:
                container_uid = 0
                container_gid = 0
            
            # Set ownership to the mapped root user inside the container
            os.chown(full_path, container_uid, container_gid)
            logger.info(f"Set file ownership to {container_uid}:{container_gid} for {path}")
                
        except PermissionError:
            logger.error(f"Permission denied while uploading file to {path}")
            raise RuntimeError("Permission denied")
        except Exception as e:
            # Ensure file is closed even on error
            try:
                await file_obj.close()
            except:
                pass
            logger.error(f"Error writing file {path}: {str(e)}")
            raise RuntimeError(f"Error writing file: {str(e)}")

    async def mkdir(self, container_id: str, path: str, parents: bool = False) -> None:
        """
        Create a directory in the container's filesystem.
        Sets directory permissions to 755 and ownership to the mapped root user inside the container.
        
        Args:
            container_id: The ID of the container
            path: The path where the directory should be created
            parents: If True, create parent directories as needed
        """
        container_info = await self._get_container_info(container_id)
        merged_dir = container_info["GraphDriver"]["Data"]["MergedDir"]
        full_path = os.path.join(merged_dir, path.lstrip('/'))
        
        # Get the mapped UID/GID for root (0) inside the container
        if ContainerEngine.USE_UIDMAP:
            uid_map = container_info["HostConfig"]["IDMappings"]["UidMap"][0]
            gid_map = container_info["HostConfig"]["IDMappings"]["GidMap"][0]
            # Parse the mapping (format: "container_id:host_id:size")
            container_uid = int(uid_map.split(':')[1])
            container_gid = int(gid_map.split(':')[1])
        else:
            container_uid = 0
            container_gid = 0
        
        try:
            if parents:
                logger.info(f"Creating directory with parents in container {container_id}: {path}")
                # Create all parent directories
                os.makedirs(full_path, exist_ok=True)
                # Set permissions and ownership for all created directories
                current = full_path
                while current != merged_dir:
                    os.chmod(current, 0o755)  # rwxr-xr-x
                    os.chown(current, container_uid, container_gid)  # mapped root:root
                    logger.info(f"Set directory permissions to 755 and ownership to {container_uid}:{container_gid} for {current}")
                    current = os.path.dirname(current)
            else:
                logger.info(f"Creating directory in container {container_id}: {path}")
                # Check if parent directory exists
                parent_dir = os.path.dirname(full_path)
                if not os.path.exists(parent_dir):
                    logger.error(f"Parent directory does not exist: {parent_dir}")
                    raise RuntimeError("Parent directory does not exist")
                os.mkdir(full_path)
                # Set permissions and ownership for the new directory
                os.chmod(full_path, 0o755)  # rwxr-xr-x
                os.chown(full_path, container_uid, container_gid)  # mapped root:root
                logger.info(f"Set directory permissions to 755 and ownership to {container_uid}:{container_gid} for {path}")
                
        except PermissionError:
            logger.error(f"Permission denied while creating directory {path}")
            raise RuntimeError("Permission denied")
        except FileExistsError:
            logger.error(f"Directory already exists: {path}")
            raise RuntimeError("Directory already exists")
        except Exception as e:
            logger.error(f"Error creating directory {path}: {str(e)}")
            raise RuntimeError(f"Error creating directory: {str(e)}")

    async def pause_sandbox(self, container_id: str) -> None:
        try:
            await self._run_podman_command(['pause', container_id])
            logger.info(f"Paused container {container_id}")
        except Exception as e:
            logger.error(f"Error pausing container {container_id}: {str(e)}")
            raise RuntimeError(f"Error pausing container: {str(e)}")

    async def unpause_sandbox(self, container_id: str) -> None:
        try:
            await self._run_podman_command(['unpause', container_id])
            logger.info(f"Unpaused container {container_id}")
        except Exception as e:
            logger.error(f"Error unpausing container {container_id}: {str(e)}")
            raise RuntimeError(f"Error unpausing container: {str(e)}")