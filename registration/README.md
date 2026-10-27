# Registration record

The study was registered before any outcome was computed, and every later amendment and erratum was registered before it
was applied. This folder holds the record.

- `study1_registration.txt`: random memecoin bets (Study 1).
- `study2_registration.txt`: memecoin retail positions (Study 2).
- `study3_registration.txt`: the three-venue study (Study 3).
- `freeze_record.txt`: the hash record. Each entry is dated and lists the SHA-256 of the registration and code files as they
  stood at that point. The code paths in it refer to the original research repository, whose scripts were later cleaned
  into the `python/` folder here, so those hashes do not match the files in this repository.
- `proofs/`: OpenTimestamps proofs (`.ots`) that anchor ten successive snapshots of the hash record in the Bitcoin
  blockchain, and `original_snapshot_hashes.txt` with the SHA-256 of each snapshot.

## Differences from the originals

These copies differ from the timestamped originals in a few places. Personal details and internal storage names are
replaced by neutral wording, and long dashes are written as hyphens. Their hashes therefore differ from the ones in the
proofs.

The last snapshot of the hash record lists the SHA-256 of each original registration:

```
114371631f81ef419c5788264dbe0d0201a168dd5d5bec865b2b9f6dc0727523  Study 1 registration
d1227456ed87ec3c86916ea4ab34e2ca8619661edb73897108db8910e1dd5ba8  Study 2 registration
b6d75477b490201429bd3e47d90dac5209376bbfac0207d0541460f9723e24ba  Study 3 registration
```

The original files are available to referees on request. With them, `ots verify <snapshot>.ots` confirms when each
snapshot existed, and the hashes above confirm the registrations it lists.
