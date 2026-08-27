// Disposable spike: validates Docker Engine SDK assumptions before C07.
// NOT part of any real package. Run with: cd spike && go run .
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	img     = "alpine:3.20"
	volName = "quarry-spike-vol"
	ctrName = "quarry-spike-ctr"
)

// The script's FIRST action is to write to stdout and stderr — if attach-before-start
// races, we'd lose "FIRST-STDOUT"/"FIRST-STDERR".
const script = `echo FIRST-STDOUT; echo FIRST-STDERR >&2;
echo "--- hello.txt:"; cat /workspace/hello.txt;
echo "--- sub/nested.txt:"; cat /workspace/sub/nested.txt;
mkdir -p /workspace/bin; printf 'produced by container\n' > /workspace/bin/out.txt;
echo LAST-STDOUT; echo LAST-STDERR >&2;
exit 3`

func step(n int, format string, a ...any) {
	fmt.Printf("\n[step %d] %s\n", n, fmt.Sprintf(format, a...))
}
func ok(format string, a ...any)   { fmt.Printf("   OK   %s\n", fmt.Sprintf(format, a...)) }
func warn(format string, a ...any) { fmt.Printf("   WARN %s\n", fmt.Sprintf(format, a...)) }
func fail(format string, a ...any) { fmt.Printf("   FAIL %s\n", fmt.Sprintf(format, a...)); os.Exit(1) }

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		fail("client: %v", err)
	}
	defer cli.Close()
	fmt.Printf("docker client api=%s\n", cli.ClientVersion())

	// Best-effort cleanup of leftovers from a previous run.
	_ = cli.ContainerRemove(ctx, ctrName, container.RemoveOptions{Force: true})
	_ = cli.VolumeRemove(ctx, volName, true)
	ensureImage(ctx, cli)

	// ---- 1. volume
	step(1, "VolumeCreate %q", volName)
	v, err := cli.VolumeCreate(ctx, volume.CreateOptions{Name: volName})
	if err != nil {
		fail("%v", err)
	}
	ok("volume %s at %s", v.Name, v.Mountpoint)

	// ---- 2. container with volume at /workspace
	step(2, "ContainerCreate %q (image %s, volume -> /workspace)", ctrName, img)
	cr, err := cli.ContainerCreate(ctx,
		&container.Config{
			Image:      img,
			Cmd:        []string{"sh", "-c", script},
			WorkingDir: "/workspace",
			Tty:        false, // must be false for stdcopy multiplexing
		},
		&container.HostConfig{
			Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: volName, Target: "/workspace"}},
		}, nil, nil, ctrName)
	if err != nil {
		fail("%v", err)
	}
	id := cr.ID
	ok("container %s (warnings=%v)", id[:12], cr.Warnings)

	// ---- 3. CopyToContainer
	step(3, "CopyToContainer -> /workspace (tar with hello.txt, sub/nested.txt)")
	tarBuf := buildTar(map[string]string{
		"hello.txt":      "hello from the host\n",
		"sub/nested.txt": "nested file\n",
	})
	if err := cli.CopyToContainer(ctx, id, "/workspace", tarBuf, container.CopyToContainerOptions{}); err != nil {
		fail("%v", err)
	}
	ok("copied %d-byte tar (no explicit dir entry for sub/ — testing implicit creation)", tarBuf.Len())

	// ---- 4. attach BEFORE start
	step(4, "ContainerAttach before start (Stream, Stdout, Stderr; Tty=false) + stdcopy.StdCopy demux")
	att, err := cli.ContainerAttach(ctx, id, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		fail("%v", err)
	}
	var stdout, stderr bytes.Buffer
	demuxDone := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, att.Reader)
		demuxDone <- err
	}()
	ok("attached; demux goroutine running")

	// ---- 5. start
	step(5, "ContainerStart")
	t0 := time.Now()
	if err := cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		fail("%v", err)
	}
	ok("started in %s", time.Since(t0).Round(time.Millisecond))

	// ---- 6. wait
	step(6, "ContainerWait(WaitConditionNotRunning)")
	waitCh, errCh := cli.ContainerWait(ctx, id, container.WaitConditionNotRunning)
	var exitCode int64 = -1
	select {
	case w := <-waitCh:
		exitCode = w.StatusCode
		if w.Error != nil {
			warn("wait response carried error: %s", w.Error.Message)
		}
	case err := <-errCh:
		fail("wait: %v", err)
	}
	ok("exited with code %d after %s (expected 3)", exitCode, time.Since(t0).Round(time.Millisecond))

	// Drain the demuxer: the hijacked conn closes when the container exits.
	select {
	case err := <-demuxDone:
		if err != nil && !errors.Is(err, io.EOF) {
			warn("StdCopy returned: %v", err)
		} else {
			ok("StdCopy returned cleanly (EOF) after container exit")
		}
	case <-time.After(5 * time.Second):
		warn("StdCopy did not return within 5s after exit — would need att.Close() to unblock")
	}
	att.Close()

	fmt.Printf("   --- demuxed STDOUT (%d bytes):\n%s", stdout.Len(), indent(stdout.String()))
	fmt.Printf("   --- demuxed STDERR (%d bytes):\n%s", stderr.Len(), indent(stderr.String()))
	check("stdout has FIRST-STDOUT (attach-before-start caught first output)", strings.Contains(stdout.String(), "FIRST-STDOUT"))
	check("stderr has FIRST-STDERR", strings.Contains(stderr.String(), "FIRST-STDERR"))
	check("stdout has LAST-STDOUT", strings.Contains(stdout.String(), "LAST-STDOUT"))
	check("stderr has LAST-STDERR", strings.Contains(stderr.String(), "LAST-STDERR"))
	check("stdout does NOT contain stderr lines (demux clean)", !strings.Contains(stdout.String(), "STDERR"))
	check("stderr does NOT contain stdout lines (demux clean)", !strings.Contains(stderr.String(), "STDOUT"))
	check("copied hello.txt was readable in container", strings.Contains(stdout.String(), "hello from the host"))
	check("copied sub/nested.txt was readable (implicit dir created)", strings.Contains(stdout.String(), "nested file"))

	// ---- 7. CopyFromContainer — several path spellings to observe tar entry naming
	step(7, "CopyFromContainer — inspect tar entry names for path doubling")
	for _, p := range []string{"/workspace/bin", "/workspace/bin/", "/workspace/bin/.", "/workspace/bin/out.txt"} {
		rc, stat, err := cli.CopyFromContainer(ctx, id, p)
		if err != nil {
			warn("path %q: %v", p, err)
			continue
		}
		names, contents := listTar(rc)
		rc.Close()
		fmt.Printf("   path %-24q stat.Name=%-8q entries=%v\n", p, stat.Name, names)
		if c, has := contents["bin/out.txt"]; has {
			fmt.Printf("      bin/out.txt content: %q\n", strings.TrimSpace(c))
		}
		for _, n := range names {
			if strings.Contains(n, "bin/bin") {
				warn("DOUBLED path segment in %q for path %q", n, p)
			}
		}
	}

	// ---- 8. remove container (force), then volume
	step(8, "ContainerRemove(Force) then VolumeRemove")
	if err := cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true}); err != nil {
		fail("container remove: %v", err)
	}
	ok("container removed")
	t1 := time.Now()
	if err := cli.VolumeRemove(ctx, volName, false); err != nil {
		warn("volume remove (force=false) FAILED on first try after %s: %v", time.Since(t1).Round(time.Millisecond), err)
		if err := cli.VolumeRemove(ctx, volName, true); err != nil {
			fail("volume remove (force=true) also failed: %v", err)
		}
		ok("volume removed with force=true")
	} else {
		ok("volume removed first try (force=false) in %s", time.Since(t1).Round(time.Millisecond))
	}

	// ---- extra: attach AFTER start, to show what we'd lose without step-4 ordering
	fmt.Println("\n[extra] control experiment: same script, attach AFTER start (Logs=false)")
	attachAfterStart(ctx, cli)

	fmt.Println("\nspike complete")
}

func attachAfterStart(ctx context.Context, cli *client.Client) {
	name := ctrName + "-late"
	_ = cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	cr, err := cli.ContainerCreate(ctx, &container.Config{
		Image: img,
		Cmd:   []string{"sh", "-c", "echo FIRST-STDOUT; echo FIRST-STDERR >&2; sleep 1; echo LAST-STDOUT"},
	}, nil, nil, nil, name)
	if err != nil {
		warn("create: %v", err)
		return
	}
	defer cli.ContainerRemove(ctx, cr.ID, container.RemoveOptions{Force: true})
	if err := cli.ContainerStart(ctx, cr.ID, container.StartOptions{}); err != nil {
		warn("start: %v", err)
		return
	}
	att, err := cli.ContainerAttach(ctx, cr.ID, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		warn("attach: %v", err)
		return
	}
	defer att.Close()
	var stdout, stderr bytes.Buffer
	done := make(chan struct{})
	go func() { _, _ = stdcopy.StdCopy(&stdout, &stderr, att.Reader); close(done) }()
	waitCh, _ := cli.ContainerWait(ctx, cr.ID, container.WaitConditionNotRunning)
	<-waitCh
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	fmt.Printf("   attach-after-start captured stdout=%q stderr=%q\n", stdout.String(), stderr.String())
	if strings.Contains(stdout.String(), "FIRST-STDOUT") {
		fmt.Println("   -> first line NOT lost this time (race not observed; do not rely on it)")
	} else {
		fmt.Println("   -> first line LOST: confirms attach must happen before start (or use Logs=true)")
	}
}

func ensureImage(ctx context.Context, cli *client.Client) {
	if _, err := cli.ImageInspect(ctx, img); err == nil {
		fmt.Printf("image %s present locally\n", img)
		return
	}
	fmt.Printf("pulling %s ...\n", img)
	rc, err := cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		fail("pull: %v", err)
	}
	_, _ = io.Copy(io.Discard, rc)
	rc.Close()
}

func buildTar(files map[string]string) *bytes.Buffer {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range files {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), ModTime: time.Now()})
		_, _ = tw.Write([]byte(body))
	}
	_ = tw.Close()
	return &buf
}

func listTar(r io.Reader) (names []string, contents map[string]string) {
	contents = map[string]string{}
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, fmt.Sprintf("%s(%c)", h.Name, h.Typeflag))
		if h.Typeflag == tar.TypeReg {
			b, _ := io.ReadAll(tr)
			contents[h.Name] = string(b)
		}
	}
	return
}

func check(what string, cond bool) {
	if cond {
		ok("%s", what)
	} else {
		warn("NOT satisfied: %s", what)
	}
}

func indent(s string) string {
	if s == "" {
		return "      (empty)\n"
	}
	return "      " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n      ") + "\n"
}
