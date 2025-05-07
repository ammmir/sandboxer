# sandboxer

https://github.com/user-attachments/assets/ed2bbf10-f430-4426-a38e-fb82e0b0c7d0

Based on https://amirmalik.net/2025/03/07/code-sandboxes-for-llm-ai-agents

**Sandboxer is a code execution sandbox server that makes it easy to execute Linux programs via HTTP.**

## Prerequisites

Sandboxer requires:
* [Podman](https://podman.io)
* [runc](https://github.com/opencontainers/runc)
* [CRIU](https://criu.org)

Optionally, for TLS and per-sandbox subdomains:
* [Caddy](https://caddyserver.com)

Install CRIU using your distribution's package manager:

```bash
add-apt-repository ppa:criu/ppa && \
apt update && \
apt-get install criu
```

Install the latest versions of the rest:

> ⚠️ **Warning:** This will overwrite OCI runtimes and configurations on your system:

```bash
wget https://github.com/mgoltzsche/podman-static/releases/download/v5.4.2/podman-linux-amd64.tar.gz && \
tar --strip-components=1 -C / -zxvf podman-linux-amd64.tar.gz
```

## Quick Start

Download the latest release:

```bash
wget https://github.com/ammmir/sandboxer/releases/download/latest/sandboxer && \
chmod 755 sandboxer && \
mv sandboxer /usr/local/sbin
```

Run it:

```bash
$ sandboxer

Initial admin token created!
Token: JL8pp3K1gZ_wHfd0G0R3DOBDt2KI-3TK08Mk4Xuel4Y
Use this token to log in to the system.

....
```

In another terminal, run Docker's Hello World inside a new sandbox:

```bash
$ curl -H "Authorization: Bearer JL8pp3K1gZ_wHfd0G0R3DOBDt2KI-3TK08Mk4Xuel4Y" \
  -H "Content-type: application/json" \
  -d '{"image": "hello-world", "label": "hello", "interactive": false}' \
  http://localhost:8000/sandboxes
{"id":"b47300fa1f01fcde12d628f88e18603fd7c68807374b63fb3cd6ffd460c3a6aa","name":"unknown","label":"hello"}
```

Now let's see its execution result:

```bash
$ curl -H "Authorization: Bearer JL8pp3K1gZ_wHfd0G0R3DOBDt2KI-3TK08Mk4Xuel4Y" \
  http://localhost:8000/sandboxes/b47300fa1f01fcde12d628f88e18603fd7c68807374b63fb3cd6ffd460c3a6aa/logs
Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (amd64)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://hub.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/get-started/root
```

## External Access

For external access, you'll need to add 2 DNS records:
* `sandboxer.example.com`
* `*.sandboxer.example.com`

These should point to the public IP address of the server Sandboxer is running.

Next, install Caddy:

```bash
wget https://github.com/caddyserver/caddy/releases/download/v2.10.0/caddy_2.10.0_linux_amd64.deb && \
dpkg -i caddy_2.10.0_linux_amd64.deb
```

Generate the Caddy configuration:

```bash
sandboxer --vhost sandboxer.example.com --generate-caddy-config > /etc/caddy/Caddyfile
```

Edit `/etc/caddy/Caddyfile` and update your DNS provider settings and specify the required email address for TLS certificate issuance. For instance, if you're using Cloudflare DNS, you'd use `dns cloudflare API_TOKEN_FROM_CLOUDFLARE`, etc.

Finally, restart Caddy:

```bash
service caddy restart
```

Now restart Sandboxer using your domain:

```bash
sandboxer --vhost sandboxer.example.com
```

You can test it out by visiting https://sandboxer.example.com and login with the admin token.

Note that Caddy requires additional configuration for automatic HTTPS with wildcard certificates. For more info see: https://caddyserver.com/docs/automatic-https#wildcard-certificates

## Client SDKs

Python (sync and async) and JavaScript/TypeScript client libraries are available under the `sdk` directory. They haven't been submitted to the relevant package managers yet.

## Tips

### Quota Support

To limit the amunt of disk space sandboxes can use, it's highly recommended to run the container backing store (`podman info -f '{{.Store.GraphRoot}}'`) on an XFS file system with project quota enabled. For testing and lightweight use, you can create a disk image and mount it using a loopback device.

For example, here's how to create a 50 GB sparse image:

```bash
dd if=/dev/zero of=/var/xfs.img bs=1M seek=50k count=0 && \
mkfs.xfs /var/xfs.img && \
mount -o rw,relatime,attr2,inode64,logbufs=8,logbsize=32k,prjquota,nodiratime,noatime /var/xfs.img /mnt/xfs
```

Update `/etc/containers/storage.conf` to use that path:

```ini
[storage]
  ...
  graphroot = "/mnt/xfs"
```

Run `podman system reset` to reconfigure the container store.


### Security

Use [gVisor](https://gvisor.dev) for a hardened container runtime. After installation, ensure `/etc/containers/containers.conf` contains:

```ini
[engine]
runtime = "runsc"
````

Subsequently created sandboxes will automatically use gVisor as the container runtime.

Otherwise, even though Sandboxer attempts to drop all capabilities and only adds necessary ones, you'll still have to do some hardening on the host side (e.g., restrict syslog access with `sysctl -w kernel.dmesg_restrict=1`, and probably more.)