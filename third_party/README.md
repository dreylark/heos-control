# Third-party notices

`licenses/` contains the license and notice files collected from the Go packages
linked into `cmd/heos-control`, plus the Go standard library license. The service's
own code is covered by the root MIT license. These third-party works retain their
own terms; the root license does not replace them.

The runtime image includes these files under
`/usr/share/licenses/heos-control/`, together with the Debian CA-bundle copyright
notice and MPL 2.0 text from the pinned builder image. Scalar is fetched by the
browser from its pinned CDN; its bundle is not redistributed in the image.

Regenerate the dependency notices after changing shipped dependencies, using the
pinned audit tool without adding it to the runtime module:

```sh
go run github.com/google/go-licenses/v2@v2.0.1 save ./cmd/heos-control \
  --save_path=/tmp/heos-license-review \
  --ignore=github.com/dreylark/heos-control
```

Review that output against `licenses/` before replacing files. The generator does
not collect the Go standard library license; refresh `licenses/go/LICENSE` from
the selected Go toolchain separately. Compiler, generator, development-image and
test-only dependencies are not part of the runtime notice set. The scanner reports
assembly inspection limits for xxhash and x/sys; their notices are included.

If distributing a standalone binary archive, include the root `LICENSE` and this
directory alongside it. Helm archives include their own copy of the project
license. License collection is a maintenance check, not a substitute for reviewing
new dependency or bundled-asset terms.
