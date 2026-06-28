# Empty Bhavcopy Cache Guard

Date: 2026-06-28

Context:

- A clean history build failed on `NSE/cm06SEP1995bhav.csv.zip` because the cached file was zero bytes.
- The file looked like an existing ZIP by name, so fetch skipped it and build later failed with `File is not a zip file`.

Change:

- `history-fetch` no longer treats zero-byte existing files as valid cache hits.
- Empty responses are not persisted as raw bhavcopy files.
- `history-build` treats leftover zero-byte raw files as non-bhavcopy inputs so the build can continue and the audit can report the missing date.

Verification:

- Focused tests cover zero-byte cache refetch and zero-byte build rejection.
- Full test suite passed.
- NSE raw cache scan found no remaining zero-byte files after the targeted refetch attempt removed the bad file.
