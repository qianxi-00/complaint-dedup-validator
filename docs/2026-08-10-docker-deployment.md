# Docker Deployment Plan

**Goal:** Deploy the FastAPI application to `192.168.16.69:28765` while keeping dependencies, source code, secrets, and runtime data independently replaceable.

1. Add a dependency-only Docker image and a Compose definition using bind mounts.
2. Verify the existing test suite, Compose rendering, image build, and local container health.
3. Create the isolated server directory layout and securely transfer code and configuration.
4. Start the service and verify container health plus HTTP access from the server and workstation.
