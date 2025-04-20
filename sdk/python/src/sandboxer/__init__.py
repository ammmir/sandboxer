import httpx

class Sandbox:
    def __init__(self, client, id: str, label: str = None, image: str = None, interactive: bool = False, **kwargs):
        self.client = client
        self.id = id
        self.label = label
        self.image = image
        self.interactive = interactive
        self._data = kwargs

    def fork(self, label: str):
        response = self.client._sync.post(f"/sandboxes/{self.id}/fork", json={"label": label})
        response.raise_for_status()
        return Sandbox(self.client, **response.json())

    async def afork(self, label: str):
        response = await self.client._async.post(f"/sandboxes/{self.id}/fork", json={"label": label})
        response.raise_for_status()
        return Sandbox(self.client, **response.json())

    def exec(self, code: str):
        response = self.client._sync.post(f"/sandboxes/{self.id}/execute", json={"code": code})
        response.raise_for_status()
        return response.json()

    async def aexec(self, code: str):
        response = await self.client._async.post(f"/sandboxes/{self.id}/execute", json={"code": code})
        response.raise_for_status()
        return response.json()

    def label(self, label: str):
        response = self.client._sync.patch(f"/sandboxes/{self.id}", json={"label": label})
        response.raise_for_status()

    def terminate(self):
        response = self.client._sync.delete(f"/sandboxes/{self.id}")
        response.raise_for_status()

    async def aterminate(self):
        response = await self.client._async.delete(f"/sandboxes/{self.id}")
        response.raise_for_status()

    def tree(self, dead_prefix: str = ""):
        response = self.client._sync.get(f"/sandboxes/{self.id}/tree")
        response.raise_for_status()
        return response.json()

    async def atree(self, dead_prefix: str = ""):
        response = await self.client._async.get(f"/sandboxes/{self.id}/tree")
        response.raise_for_status()
        return response.json()

    def coalesce(self, final_sandbox):
        response = self.client._sync.post(f"/sandboxes/{self.id}/coalesce/{final_sandbox.id}")
        response.raise_for_status()
        return Sandbox(self.client, **response.json())

    async def acoalesce(self, final_sandbox):
        response = await self.client._async.post(f"/sandboxes/{self.id}/coalesce/{final_sandbox.id}")
        response.raise_for_status()
        return Sandbox(self.client, **response.json())

    def logs(self, limit: int = 1000):
        """Return all execution logs for this sandbox."""
        response = self.client._sync.get(f"/sandboxes/{self.id}/execution-logs", params={"limit": limit})
        response.raise_for_status()
        return response.json()

    async def alogs(self, limit: int = 1000):
        """Async version of logs()."""
        response = await self.client._async.get(f"/sandboxes/{self.id}/execution-logs", params={"limit": limit})
        response.raise_for_status()
        return response.json()

class Sandboxer:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {token}"}
        self._sync = httpx.Client(base_url=self.base_url, headers=self.headers)
        self._async = httpx.AsyncClient(base_url=self.base_url, headers=self.headers)

    def create(self, image: str, label: str, interactive: bool = False) -> Sandbox:
        response = self._sync.post("/sandboxes", json={
            "image": image,
            "label": label,
            "interactive": interactive
        })
        response.raise_for_status()
        return Sandbox(self, **response.json())

    async def acreate(self, image: str, label: str, interactive: bool = False) -> Sandbox:
        response = await self._async.post("/sandboxes", json={
            "image": image,
            "label": label,
            "interactive": interactive
        })
        response.raise_for_status()
        return Sandbox(self, **response.json())
