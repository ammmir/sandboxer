import asyncio
import json
from typing import AsyncIterator, List, Optional, Dict, Tuple, Union
import tarfile
import tempfile
import os
import shutil
import subprocess
import uuid

class Container:
    def __init__(self, engine: 'ContainerEngine', **kwargs):
        self.engine = engine
        self.id = kwargs.get("id", "unknown")
        self.ip_address = kwargs.get("ip_address", None)
        self.name = kwargs.get("name", "unknown")
        self.status = kwargs.get("status", "unknown")
        self.image = kwargs.get("image", "unknown")
        self.image_id = kwargs.get("image_id", "unknown")
        self.created_at = kwargs.get("created_at", -1)
        self.started_at = kwargs.get("started_at", -1)
        self.exited_at = kwargs.get("exited_at", -1)
        self.command = kwargs.get("command", [])

    async def exec(self, command: Union[str, List[str]], stream: bool = False) -> Union[Tuple[str, str, int], AsyncIterator[Tuple[str, str, int]]]:
        """Execute a command inside the container."""
        return await self.engine.exec(self.id, command, stream=stream)

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
            ip_address = await self._get_container_ip(data["Id"]) if data["State"] == "running" else "N/A"
            containers.append(
                Container(
                    id=data["Id"],
                    ip_address=ip_address,
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
            args.extend(['sh', '-c', command])  # Use shell for string commands
        else:
            args.extend(command)  # Pass list directly to avoid shell injection

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

    async def start_container(self, image: str, name: str) -> Container:
        """
        Start a new container using `podman run --rm -d`.
        Returns a `Container` object with its assigned IP address.
        """
        args = [
            'run',
            '--rm',  # Auto-remove container after stopping
            '-d',  # Run in detached mode
            '--security-opt', 'seccomp=unconfined',
            '--name', name,  # Container name
            f'docker://{image}'  # The image to run
        ]
        
        container_id = await self._run_podman_command(args)
        print(f'started container: {container_id}')
        container_ip = await self._get_container_ip(container_id)
        print(f'container ip: {container_ip}')

        return Container(id=container_id, ip_address=container_ip, engine=self)

    async def _get_container_ip(self, container_id: str) -> str:
        """
        Inspect a container and return its assigned IP address.
        """
        output = await self._run_podman_command(['inspect', container_id])
        container_info = json.loads(output)
        ip_address = container_info[0]['NetworkSettings']['IPAddress']
        return ip_address

    async def get_container(self, container_id: str) -> Optional[Container]:
        """Inspect a container and return a `Container` object."""
        try:
            output = await self._run_podman_command(['inspect', container_id])
            data = json.loads(output)[0]

            return Container(
                engine=self,
                id=data["Id"],
                ip_address=data['NetworkSettings']['IPAddress'] if data["State"] == "running" else None,
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
            'container', 'checkpoint', '-e', image_path, '--leave-running', '--ignore-volumes', container_id
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

    def _edit_checkpoint(self, snapshot_file: str, volume_map: Dict[str, str], new_volumes: Dict[str, str]) -> str:
        """Extract, modify, and repack the checkpoint to update volume references."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract checkpoint
            with tarfile.open(snapshot_file, "r:*") as tar:
                tar.extractall(temp_dir)

            # Perform search & replace for volume IDs in config.dump & spec.dump
            for file_name in ["config.dump", "spec.dump"]:
                file_path = os.path.join(temp_dir, file_name)

                with open(file_path, "r") as f:
                    content = f.read()

                # Replace old volume IDs with new ones
                for mount_path, old_volume in volume_map.items():
                    new_volume = new_volumes[mount_path]
                    content = content.replace(old_volume, new_volume)
                    content = content.replace(f"/var/lib/containers/storage/volumes/{old_volume}/_data",
                                              f"/var/lib/containers/storage/volumes/{new_volume}/_data")

                with open(file_path, "w") as f:
                    f.write(content)

            # Repack the modified checkpoint
            modified_snapshot = f"{snapshot_file}.modified.tar.gz"
            with tarfile.open(modified_snapshot, "w:gz") as tar:
                tar.add(temp_dir, arcname=".")

        return modified_snapshot

    async def fork_container(self, container_id: str, new_name: str) -> str:
        """
        Forks a running container by:
        1. Creating a snapshot
        2. Duplicating volumes
        3. Editing the checkpoint metadata
        4. Restoring the container with new volumes
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

        return Container(id=restored_id, ip_address=container_ip, engine=self)

    async def close(self):
        """Placeholder for cleanup if needed later."""
        pass