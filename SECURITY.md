# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.2.x   | Yes                |
| < 0.2   | No                 |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, email **krishna19.gk@gmail.com** with:

1. Description of the vulnerability
2. Steps to reproduce
3. Potential impact
4. Suggested fix (if any)

You will receive a response within 48 hours. If the issue is confirmed, a fix will be released as a patch version and you will be credited in the changelog (unless you prefer to remain anonymous).

## Security Considerations

This project interacts with AWS QuickSight APIs. Users should be aware of:

- **AWS Credentials**: Never commit AWS access keys, secret keys, session tokens, or account IDs. Use environment variables or AWS profiles.
- **Analysis Definitions**: QuickSight analysis definitions may contain dataset ARNs that encode account IDs. These are operational identifiers, not secrets, but avoid sharing them publicly.
- **Backup Files**: Backups saved to `~/.quicksight-mcp/backups/` contain full analysis definitions. Treat them as sensitive data.
- **SPICE Data**: This server can trigger SPICE refreshes that process potentially sensitive business data. Ensure your AWS IAM permissions follow the principle of least privilege.
