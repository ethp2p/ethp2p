Refer to our [stance on intellectual property](../README.md#stance-on-intellectual-property).

---

### Optional RLNC Benchmarks

RLNC benchmark support is intentionally excluded from normal public builds. If
the private `../ethp2p-extras` repository is present, add it to a local Go
workspace and enable the `rlnc` build tag:

```bash
just link-rlnc
go test -tags rlnc ./sim
```

Normal `go test ./...` runs without the private implementation.
