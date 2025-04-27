import asyncio
from pathlib import Path
import aiofiles

class SubordinateManager:
    BLOCK_SIZE = 1000

    def __init__(
        self,
        subuid_path: str = "/etc/subuid",
        subgid_path: str = "/etc/subgid",
        base_uid: int = 1_000_000,
        base_gid: int = 1_000_000,
    ):
        self.subuid_path = Path(subuid_path)
        self.subgid_path = Path(subgid_path)
        self.base_uid = base_uid
        self.base_gid = base_gid
        self._lock = asyncio.Lock()

    async def append_subuid(self, user: str, id: int) -> None:
        uid = self.base_uid + id * SubordinateManager.BLOCK_SIZE
        line = f"{user}:{uid}:{SubordinateManager.BLOCK_SIZE}\n"
        async with self._lock:
            async with aiofiles.open(self.subuid_path, mode="a") as f:
                await f.write(line)

    async def append_subgid(self, user: str, id: int) -> None:
        gid = self.base_gid + id * SubordinateManager.BLOCK_SIZE
        line = f"{user}:{gid}:{SubordinateManager.BLOCK_SIZE}\n"
        async with self._lock:
            async with aiofiles.open(self.subgid_path, mode="a") as f:
                await f.write(line)

    def get_subuid(self, id: int) -> int:
        return self.base_uid + id * SubordinateManager.BLOCK_SIZE

    def get_subgid(self, id: int) -> int:
        return self.base_gid + id * SubordinateManager.BLOCK_SIZE
