# Security Policy

## Reporting Security Vulnerabilities

The ObsiFlow project team takes security and data integrity seriously. If you discover a security vulnerability or potential risk, please report it privately.

**Please do not report security vulnerabilities through public GitHub issues or pull requests.**

### How to Report
To report a vulnerability:
1. Use GitHub's **Private Vulnerability Reporting** feature via the repository's **Security** tab:
   - Navigate to `Security` > `Advisories` > `Report a vulnerability`.
2. Alternatively, reach out directly and privately to the project maintainers and Team Leader (**Mohammad Maher Metwaly**) through official academic / team communication channels.

### Information to Include
To help us evaluate and address the issue efficiently, please include:
- A clear description of the vulnerability and its potential impact.
- Step-by-step reproduction instructions or a minimal proof-of-concept.
- The affected component, file, or configuration (e.g., Airflow orchestration, PostgreSQL configuration, data ingestion, or credential externalization).
- Any potential mitigations or suggested fixes.

The project team will acknowledge receipt of your report, assess the severity, and coordinate a fix in a private branch before publishing an update.

---

## Security Expectations for Contributors

### 1. Credentials & Sensitive Data
- **Never commit credentials**: Passwords, API keys, tokens, SSH keys, and production connection strings must never be committed to git.
- **Environment variables**: Use `.env` files for local configuration and keep `.env` excluded from version control. Always provide sanitized dummy values in `.env.example`.
- **Database passwords**: Use secure environment variables for PostgreSQL / database access credentials.

### 2. Data Privacy & Synthetic Datasets
- This platform operates on synthetic financial datasets generated via `data/genration_script/generate_data.py`. Real customer records or proprietary banking information must never be committed or processed in test environments.

### 3. Safe Dependency Management
- Only pin stable and verified dependencies in `requirements.txt`.
- Avoid adding unvetted third-party packages or untrusted container images.

### 4. Input Validation & Safe Querying
- All SQL queries must use parameterized interfaces to prevent SQL injection.
- Validate all incoming batch payloads against data-quality constraints before persistence.
