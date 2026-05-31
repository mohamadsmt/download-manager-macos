# Third-Party Notices

The release DMG may include third-party binaries staged from Homebrew. The
project source does not vendor those binaries.

## aria2

- Component: `aria2c`
- Version: 1.37.0, Homebrew formula revision 1.37.0_2
- License: GPL-2.0-or-later
- Source: <https://github.com/aria2/aria2/releases/tag/release-1.37.0>
- Source archive checksum: `60a420ad7085eb616cb6e2bdf0a7206d68ff3d37fb5a956dc44242eb2f79b66b`

## Runtime libraries staged with aria2

The DMG stages Homebrew runtime libraries required by the bundled `aria2c`.
Their license files are available from the Homebrew packages and upstream
projects:

- c-ares 1.34.6: <https://c-ares.org/>
- gettext 1.0: <https://www.gnu.org/software/gettext/>
- libssh2 1.11.1_1: <https://libssh2.org/>
- OpenSSL 3.6.2: <https://www.openssl.org/>
- SQLite 3.53.0: <https://www.sqlite.org/>

The staged binary paths and checksums should be regenerated for each release by
running `Packaging/stage_aria2_homebrew.sh` on the release machine.
