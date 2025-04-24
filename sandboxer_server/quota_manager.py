import os
import subprocess
import asyncio
import tempfile
import aiofiles
import logging
from typing import Optional, Tuple
from pathlib import Path

from uvicorn.config import LOGGING_CONFIG
from uvicorn.logging import DefaultFormatter

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("uvicorn.error")

class QuotaManager:
    def __init__(self, projects_file: str = "/etc/projects", projid_file: str = "/etc/projid"):
        self.projects_file = Path(projects_file)
        self.projid_file = Path(projid_file)
        self._next_projid = 10000  # Starting project ID
        self._lock = asyncio.Lock()
        self._quota_supported = False

        logger.info("Checking XFS quota support")

        # Check quota support
        try:
            # Check if filesystem is mounted with quota support
            with open('/proc/mounts') as f:
                for line in f:
                    if 'xfs' in line and 'prjquota' in line: # FIXME: check actual podman graph dir
                        self._quota_supported = True
                        break
        except (FileNotFoundError, PermissionError):
            pass
        finally:
            logger.info(f'XFS quota support: {self._quota_supported}')
        
        # Ensure files exist
        self.projects_file.touch(exist_ok=True)
        self.projid_file.touch(exist_ok=True)
        
        # Find highest existing project ID
        if self.projid_file.exists():
            with open(self.projid_file) as f:
                for line in f:
                    if ':' in line:
                        try:
                            projid = int(line.split(':')[1])
                            self._next_projid = max(self._next_projid, projid + 1)
                        except ValueError:
                            continue

    def is_supported(self) -> bool:
        return False
        return self._quota_supported

    async def _get_next_projid(self) -> int:
        """Get the next available project ID."""
        # FIXME: doesn't handle rollover and existing allocations
        projid = self._next_projid
        if projid > 10000000:  # Reset if we hit the limit
            self._next_projid = 10000
            projid = 10000
        else:
            self._next_projid += 1
        return projid

    async def add_project(self, projname: str, block_hard: int = 5*1024*1024, inode_hard: int = 10000) -> int:
        async with self._lock:
            # Check if project already exists
            existing_projid = await self.get_projid(projname)
            if existing_projid is not None:
                logger.info(f"Project {projname} already exists with ID {existing_projid}")
                return existing_projid

            # Get next project ID
            projid = await self._get_next_projid()
            
            # Add the mapping
            async with aiofiles.open(self.projid_file, 'a') as f:
                await f.write(f"{projname}:{projid}\n")

            # Set quota limits
            await self._run_xfs_quota(f'project -s {projname}')
            await self._run_xfs_quota(f'limit -p bhard={block_hard} ihard={inode_hard} {projname}')
            
            logger.info(f"Added project {projname} with ID {projid}")
            return projid

    async def remove_project(self, projname: str) -> None:
        """Remove a project name to ID mapping from /etc/projid."""
        async with self._lock:
            await self._run_xfs_quota(f'limit -p bhard=0 ihard=0 {projname}')

            async with aiofiles.open(self.projid_file) as f:
                lines = await f.readlines()
            
            # Create temporary file in the same directory
            tmp = tempfile.NamedTemporaryFile(mode='w', dir=self.projid_file.parent, delete=False)
            try:
                # Write all lines except the one for this project
                for line in lines:
                    if not line.startswith(f"{projname}:"):
                        tmp.write(line)
                tmp.close()
                
                # Atomic rename
                os.rename(tmp.name, self.projid_file)
            except Exception:
                # Clean up temp file on error
                os.unlink(tmp.name)
                raise

    async def add_projid_path(self, projid: int, path: str) -> None:
        """Add a project ID to path mapping to /etc/projects."""
        async with self._lock:
            async with aiofiles.open(self.projects_file, 'a') as f:
                await f.write(f"{projid}:{path}\n")

    async def remove_projid_path(self, path: str) -> None:
        """Remove a project ID to path mapping from /etc/projects."""
        async with self._lock:
            async with aiofiles.open(self.projects_file) as f:
                lines = await f.readlines()
            
            # Create temporary file in the same directory
            tmp = tempfile.NamedTemporaryFile(mode='w', dir=self.projects_file.parent, delete=False)
            try:
                # Write all lines except the one for this path
                for line in lines:
                    if not line.endswith(f":{path}\n"):
                        tmp.write(line)
                tmp.close()
                
                # Atomic rename
                os.rename(tmp.name, self.projects_file)
            except Exception:
                # Clean up temp file on error
                os.unlink(tmp.name)
                raise

    async def _run_xfs_quota(self, *args: str) -> None:
        """Run xfs_quota command asynchronously."""
        cmd = ['xfs_quota', '-x', '-c', *args, '/'] # FIXME: use actual XFS mount point
        logger.info("Running xfs_quota command: %s", ' '.join(cmd))
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        
        if stdout:
            logger.info("xfs_quota stdout: %s", stdout.decode())
        if stderr:
            logger.info("xfs_quota stderr: %s", stderr.decode())
            
        if proc.returncode != 0:
            error_msg = stderr.decode()
            logger.error("xfs_quota failed: %s", error_msg)
            raise RuntimeError(f"xfs_quota failed: {error_msg}")

    async def get_projid(self, projname: str) -> Optional[int]:
        """Look up project ID for a given project name."""
        async with aiofiles.open(self.projid_file) as f:
            lines = await f.readlines()
        
        for line in lines:
            if line.startswith(f"{projname}:"):
                try:
                    return int(line.split(':')[1].strip())
                except (ValueError, IndexError):
                    continue
        return None

    async def apply(self, dir_path: str, projname: str, block_hard: int = 5*1024*1024, inode_hard: int = 10000) -> Optional[int]:
        """
        Set quota for a directory.
        
        Args:
            dir_path: Path to the directory
            projname: Project name (typically sandbox ID)
            
        Returns:
            Project ID if successful, None if not supported
        """
        if not self._quota_supported:
            return None
            
        try:
            # Look up project ID
            projid = await self.get_projid(projname)
            if projid is None:
                raise RuntimeError(f"Project {projname} not found")
            
            # Add path mapping
            await self.add_projid_path(projid, dir_path)
            
            return projid
        except Exception as e:
            # Clean up on failure
            try:
                await self.remove_projid_path(dir_path)
            except Exception:
                pass  # Ignore cleanup errors
            raise RuntimeError(f"Failed to set quota: {e}")

    async def apply2(self, projname: str) -> None:
        """
        Apply quota project settings after container startup.
        This should be called after the container is fully started to ensure
        the mount points are properly set up.
        
        Args:
            projname: Project name (typically sandbox ID)
        """
        if not self._quota_supported:
            return

        try:
            await self._run_xfs_quota(f'project -s {projname}')
        except Exception as e:
            raise RuntimeError(f"Failed to apply quota project settings: {e}") 

    async def clear_path(self, dir_path: str) -> None:
        """
        Clear quota for a project.
        
        Args:
            projname: Project name (typically sandbox ID)
            dir_path: Path to the directory
        """
        if not self._quota_supported:
            return

        try:
            await self.remove_projid_path(dir_path)
        except Exception as e:
            raise RuntimeError(f"Failed to clear quota: {e}")