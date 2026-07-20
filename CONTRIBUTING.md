# Contributing to Real-time Retail Analytics Pipeline

Thank you for your interest in contributing to the **Real-time Retail Analytics Pipeline**! To ensure a smooth collaboration, please follow the guidelines outlined below.

## Code of Conduct

Please maintain a respectful, welcoming, and inclusive environment. Avoid harassment, discrimination, or abusive behavior in all forms of communication.

## Development Workflow

1. **Fork and Clone**:

   ```bash
   git clone https://github.com/RedaAlali/realtime-retail-pipeline.git
   cd realtime-retail-pipeline
   ```

2. **Set up Environment**:
   - Copy `.env.example` to `.env` and customize your local environment values if necessary:

     ```bash
     cp .env.example .env
     ```

3. **Run with Docker Compose**:
   - Build and start all services locally to verify everything runs:

     ```bash
     docker compose build
     docker compose up
     ```

4. **Make Your Changes**:
   - Create a descriptive branch for your changes:

     ```bash
     git checkout -b feature/your-feature-name
     ```

   - Keep your code clean, modular, and write docstrings for new functions.
   - Follow PEP 8 guidelines for Python code format.

## Repository Standards

- **Folder Names**: Microservices should be located inside the `services/` directory. Database init scripts and static catalogs are kept under `db/`.
- **Code Modularization**: Avoid single-file monorepos. Split business logic, database queries, and interface views.
- **Commit Messages**: Write clear, imperative, and descriptive commit messages (e.g., `feat: add anomaly detection model to ML service`).
- **Pull Requests**: Submit a descriptive Pull Request detailing the changes, reasoning, and instructions for how to verify.
