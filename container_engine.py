import asyncio
import json
from typing import AsyncIterator, List, Optional, Dict, Tuple, Union
import tarfile
import tempfile
import os
import shutil
import subprocess
import uuid
import io

class Container:
    def __init__(self, engine: 'ContainerEngine', **kwargs):
        self.engine = engine
        self.id = kwargs.get("id", "unknown")
        self.ip_address = kwargs.get("ip_address", None)
        self.exposed_port = kwargs.get("exposed_port", None)
        self.name = kwargs.get("name", "unknown")
        self.status = kwargs.get("status", "unknown")
        self.image = kwargs.get("image", "unknown")
        self.image_id = kwargs.get("image_id", "unknown")
        self.created_at = kwargs.get("created_at", -1)
        self.started_at = kwargs.get("started_at", -1)
        self.exited_at = kwargs.get("exited_at", -1)
        self.command = kwargs.get("command", [])

    async def exec(self, command: Union[str, List[str]], stream: bool = False) -> Union[Tuple[str, str, int], AsyncIterator[Tuple[str, str, int]]]:
        return await self.engine.exec(self.id, command, stream=stream)

    async def logs(self, stream: bool = False, cancel_event: asyncio.Event = None) -> Union[str, AsyncIterator[str]]:
        return await self.engine.logs(self.id, stream=stream)

    async def stop(self) -> None:
        #await self.engine._run_podman_command(['stop', '--time', '3', self.id])
        await self.engine._run_podman_command(['kill', '-s', 'KILL', self.id])

    async def snapshot(self, image_path: str) -> None:
        await self.engine._run_podman_command([
            'container', 'checkpoint', '-e', image_path, '--leave-running', '--ignore-volumes', self.id
        ])

class ContainerEngine:
    def __init__(self, podman_path: str = 'podman --runtime /usr/sbin/run2'):
        self.podman_path = podman_path.split()

    async def _run_podman_command(self, args: List[str]) -> str:
        """Run a Podman CLI command asynchronously and return its output."""
        command = self.podman_path + args
        print(f'Executing: {" ".join(command)}')

        def run_in_thread():
            return subprocess.run(command, capture_output=True, text=True)

        try:
            process = await asyncio.to_thread(run_in_thread)
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
                    ip_address=ip_address,
                    exposed_port=exposed_port,
                    engine=self,
                    name=data["Names"][0] if data["Names"] else "unknown",
                    status=data["State"],
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

                async def read_stream(stream: Optional[asyncio.StreamReader], stream_type: str):
                    nonlocal exit_code
                    if stream:
                        async for line in stream:
                            yield stream_type, line.decode().strip(), exit_code

                async for output in read_stream(process.stdout, "stdout"):
                    yield output
                async for output in read_stream(process.stderr, "stderr"):
                    yield output

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

    async def start_container(self, image: str, name: str, env: dict[str, str] = None, args: list[str] = None, network: str = None) -> Container:
        """
        Start a new container using `podman run --rm -d`.
        Returns a `Container` object with its assigned IP address.
        """
        args_list = [
            'run',
            '--rm',  # Auto-remove container after stopping
            #'-it',  # Interactive mode with a TTY [disabled: need to implement API]
            '-d',  # Run in detached mode
            '--security-opt', 'seccomp=unconfined',
            '--name', name,  # Container name
        ]

        # Add network if specified
        if network:
            args_list.extend(['--network', network])

        # Add environment variables if provided
        if env:
            for key, value in env.items():
                args_list.extend(['-e', f'{key}={value}'])

        args_list.append(f'docker://{image}')  # The image to run

        # Add additional container arguments if provided
        if args:
            args_list.extend(args)
        
        container_id = await self._run_podman_command(args_list)
        container_ip, exposed_port = await self._get_container_ip(container_id)

        return Container(id=container_id, ip_address=container_ip, exposed_port=exposed_port, engine=self)

    async def _get_container_ip(self, container_id: str) -> Tuple[str, Optional[str]]:
        """Inspect a container and return its assigned private IP and first exposed port."""
        output = await self._run_podman_command(['inspect', container_id])
        container_info = json.loads(output)[0]

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

        return ip_address, first_exposed_port

    async def get_container(self, container_id: str) -> Optional[Container]:
        """Inspect a container and return a `Container` object."""
        try:
            output = await self._run_podman_command(['inspect', container_id])
            data = json.loads(output)[0]

            # First try the default network IP address
            ip_address = data["NetworkSettings"]["IPAddress"]
            
            # If IP is empty and there are networks, get the first network's IP
            if not ip_address and data["NetworkSettings"]["Networks"]:
                # Get the first network's IP address
                first_network = next(iter(data["NetworkSettings"]["Networks"].values()))
                ip_address = first_network.get("IPAddress", "")

            # Extract the first exposed port (even if its value is null)
            ports = data["NetworkSettings"]["Ports"]
            first_exposed_port = next(iter(ports.keys()), None) if ports else None
            first_exposed_port = first_exposed_port.split('/')[0] if first_exposed_port else None

            return Container(
                engine=self,
                id=data["Id"],
                ip_address=ip_address,
                exposed_port=first_exposed_port,
                name=data.get("Name", "").lstrip("/"),
                state=data.get("State", "unknown"),
                image=data.get("Image", "unknown"),
                image_id=data.get("ImageID", "unknown"),
                created_at=data.get("Created", -1),
                started_at=data["State"].get("StartedAt", -1),
                exited_at=data["State"].get("ExitedAt", -1),
                command=data["Config"].get("Cmd", [])
            )
        except RuntimeError:
            return None  # If the container doesn't exist

    async def stop_container(self, container_id: str) -> None:
        await self._run_podman_command(['stop', '--time', '3', container_id])

    async def remove_container(self, container_id: str) -> None:
        await self._run_podman_command(['rm', container_id])

    async def snapshot_container(self, container_id: str, image_path: str) -> None:
        await self._run_podman_command([
            'container', 'checkpoint', '-e', image_path, '--leave-running', '--ignore-volumes', '--tcp-established', '--tcp-skip-in-flight', '--ext-unix-sk', container_id
        ])

    async def restore_container(self, image_path: str, name: str) -> Container:
        """
        Restore a container from a checkpoint image.
        Returns a `Container` object with its assigned IP address.
        """
        args = ['container', 'restore', '-i', image_path, '-n', name, '--ignore-volumes']
        container_id = await self._run_podman_command(args)
        container_ip = await self._get_container_ip(container_id)

        return Container(id=container_id, ip_address=container_ip, engine=self)

    async def _get_container_volumes(self, container_id: str) -> Dict[str, str]:
        """Find named volumes attached to a container."""
        output = await self._run_podman_command(['inspect', container_id])
        container_info = json.loads(output)[0]

        volume_map = {}
        for mount in container_info.get("Mounts", []):
            if mount["Type"] == "volume":
                volume_map[mount["Destination"]] = mount["Name"]
        return volume_map

    async def _duplicate_volumes(self, container_id: str, volume_map: Dict[str, str]) -> Dict[str, str]:
        """Duplicate container volumes by creating new ones and copying data."""
        new_volumes = {}

        for mount_path, old_volume in volume_map.items():
            new_volume = f"{old_volume}_fork_{str(uuid.uuid4())[:8]}"

            # Create the new volume
            await self._run_podman_command(['volume', 'create', '--label', f'sbx_id={container_id}', new_volume])

            # Copy the data using export/import
            export_cmd = ["podman", "volume", "export", old_volume]
            import_cmd = ["podman", "volume", "import", new_volume, "-"]

            with subprocess.Popen(export_cmd, stdout=subprocess.PIPE) as export_proc, \
                 subprocess.Popen(import_cmd, stdin=export_proc.stdout) as import_proc:
                export_proc.stdout.close()
                import_proc.wait()

            new_volumes[mount_path] = new_volume

        return new_volumes

    def _edit_checkpoint(self, snapshot_file: str, volume_map: Dict[str, str], new_volumes: Dict[str, str]) -> None:
        """Modify the checkpoint tar file by creating a new one with updated volume references."""
        temp_output = f"{snapshot_file}.tmp"
        
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
                                    f"/var/lib/containers/storage/volumes/{old_volume}/_data",
                                    f"/var/lib/containers/storage/volumes/{new_volume}/_data"
                                )
                            
                            # Create a new tarinfo with the same metadata but updated size
                            new_content = content.encode('utf-8')
                            new_info = tarfile.TarInfo(member.name)
                            new_info.size = len(new_content)
                            new_info.mode = member.mode
                            new_info.uid = member.uid
                            new_info.gid = member.gid
                            new_info.mtime = member.mtime
                            
                            # Add modified content to archive
                            output_archive.addfile(new_info, fileobj=io.BytesIO(new_content))
                        else:
                            # For all other files, copy them directly
                            output_archive.addfile(member, file)
            
            # Replace the original file with our modified version
            os.replace(temp_output, snapshot_file)
            
        except Exception as e:
            # Clean up temp file if something goes wrong
            if os.path.exists(temp_output):
                os.unlink(temp_output)
            raise e
        
        return snapshot_file

    async def fork_container(self, container_id: str, new_name: str, keep_snapshot: bool = False) -> Tuple[Container, str]:
        """
        Forks a running container by:
        1. Creating a snapshot
        2. Duplicating volumes
        3. Editing the checkpoint metadata
        4. Restoring the container with new volumes

        Returns:
        - A tuple containing:
        - `Container`: The new forked container.
        - `str`: Path to the modified snapshot file.

        If `keep_snapshot=True`, the snapshot file will be retained after forking.
        """
        snapshot_file = f"{new_name}.tar.gz"

        # Step 1: Get container volume mappings
        volume_map = await self._get_container_volumes(container_id)

        # Step 2: Create snapshot
        await self._run_podman_command(['container', 'checkpoint', '-c', 'gzip', '-e', snapshot_file, '--ignore-volumes', '--leave-running', container_id])

        # Step 3: Duplicate volumes
        new_volumes = await self._duplicate_volumes(container_id, volume_map)

        # Step 4: Modify the snapshot metadata
        modified_snapshot = self._edit_checkpoint(snapshot_file, volume_map, new_volumes)

        # Step 5: Restore the modified checkpoint
        restored_id = await self._run_podman_command(['container', 'restore', '-i', modified_snapshot, '--ignore-volumes', '-n', new_name])

        container_ip = await self._get_container_ip(restored_id)

        # Delete snapshot if requested
        if not keep_snapshot:
            try:
                os.remove(modified_snapshot)
            except OSError as e:
                print(f"Failed to delete snapshot {modified_snapshot}: {e}")

        return Container(id=restored_id, ip_address=container_ip, engine=self), modified_snapshot

    async def close(self):
        """Placeholder for cleanup if needed later."""
        pass