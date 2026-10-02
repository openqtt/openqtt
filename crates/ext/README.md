# openqtt-ext

The traits a build of OpenQTT implements at compile time to authenticate and authorize
clients, hear about sessions, forward messages outside the cluster, read the log and redirect
clients. Report R3 (`docs/reports/R03-roles-and-wire.md`, section "The extension seam")
describes where each one is called.

## Compatibility

Other builds compile against this crate, so a change to it is a change to public API:

- every public struct and enum is `#[non_exhaustive]`, and comes with constructors, so a field
  or a variant can be added without breaking an implementation;
- a trait method added later comes with a default;
- `API_VERSION` names the version of the seam. Its minor number goes up when something is
  added, its major number when an implementation written against the previous version may no
  longer compile or may behave wrongly. A build checks it at compile time:
  `const _: () = assert!(openqtt_ext::API_VERSION.satisfies(openqtt_ext::ApiVersion::new(1, 0)));`

cargo-semver-checks compares the crate with `main` and refuses a breaking change:

```console
cargo install --locked cargo-semver-checks
git fetch origin main
make semver
```

`make semver` checks the change as a minor release, since the crate's own version
(`2.0.0-alpha.0`, the workspace's) does not move from one commit to the next and would let any
change through. A change that breaks the seam on purpose raises the major number of
`API_VERSION` and runs `make semver SEMVER_RELEASE=major`, which then passes.
