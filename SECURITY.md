# Security policy

## Supported versions

Security fixes are provided for the latest released minor version.

## Reporting

Please use GitHub's
[private vulnerability reporting](https://github.com/appleweiping/corpusledger/security/advisories/new). Do not open a
public issue containing a suspected secret, private corpus, exploit, or identifying data. Include affected version,
reproduction with synthetic data, impact, and a suggested mitigation if available.

CorpusLedger privacy findings are heuristic warnings, not a credential scanner or data-loss-prevention guarantee.
Manifests may be sensitive metadata. Review them before publishing and store them with protections appropriate to the
underlying corpus.

## Manifest authenticity

The optional signing extra creates detached Ed25519 signatures over exact manifest bytes. Verification is meaningful
only when the public key was obtained through a trusted channel independent of the manifest and its signature envelope.
The envelope's `key_id` detects an unexpected key; it is not itself a trust anchor.

- Keep private keys outside the repository and corpus directory, with operating-system access controls.
- Prefer encrypted PKCS#8 PEM keys. Supply the password through `--password-env`; do not place it in shell history,
  command arguments, configuration committed to Git, or CI logs.
- Rotate a key if its private material may have been disclosed, redistribute the new public-key fingerprint through the
  trusted channel, and re-sign affected release manifests.
- A signature proves possession of a key and integrity of bytes. It does not certify dataset quality, ownership,
  licensing, privacy, completeness, or timestamp.

Private and public key bytes are never written to a manifest or signature envelope. Expected errors do not contain key
contents or passwords. Key file paths may appear in local error messages, so avoid sensitive information in filenames.

## Reader plugins

Reader entry points are third-party Python code with the permissions of the CorpusLedger process. They are never loaded
automatically: `--reader NAME` or an explicit Python API injection is required. Install only reviewed adapters, pin
their distributions, and verify their recorded name/version before reproducing a manifest. Adapter metadata establishes
reproducibility, not sandboxing or publisher trust.
