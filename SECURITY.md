# Security Policy

## Supported versions

Security fixes are provided for the latest published release and the current
`main` branch. Older releases may not receive backported fixes; users should
upgrade to the latest release before reporting or validating an issue.

## Reporting a vulnerability

Please do not disclose suspected vulnerabilities in a public issue, pull
request, discussion, log, screenshot, or chat message.

Use GitHub private vulnerability reporting when it is available:

<https://github.com/Yamakitsu/qodex-bridge/security/advisories/new>

If that page does not offer a private report form, open a public issue that
contains only the title `Security contact request`, the affected version, and
a request for a private contact channel. Do not include technical details,
proof-of-concept code, credentials, tokens, user messages, local paths, or
other sensitive data in that issue.

Include the following information in the eventual private report:

- the affected version or commit;
- the component and configuration involved;
- the security impact and required preconditions;
- minimal reproduction steps or a proof of concept with secrets removed;
- any suggested mitigation, if known.

Particularly relevant reports include authentication or authorization bypass,
sandbox or project-boundary escape, unsafe command execution, path traversal,
arbitrary file access, token exposure, attachment-handling vulnerabilities,
cross-chat data leakage, and OneBot or WebUI input that crosses a trust
boundary unexpectedly.

## Response process

The maintainer will acknowledge a private report when practical, reproduce and
assess the issue, prepare a fix, and coordinate disclosure after a corrected
release is available. Please allow reasonable time for investigation before
publishing details.

## Scope and safe testing

Test only systems, accounts, repositories, QQ bots, and data that you own or
are explicitly authorized to test. Do not access other users' messages or
files, disrupt services, retain personal data, or use live credentials in a
proof of concept.

The project does not currently offer a paid bug bounty. Good-faith reports
that follow this policy are welcome.
