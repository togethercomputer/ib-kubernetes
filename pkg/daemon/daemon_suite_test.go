package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	netapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	"github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/scheme"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var cfg *rest.Config
var kubernetesClient client.Client
var netClient netclientset.Interface
var testEnv *envtest.Environment
var ctx context.Context
var cancel context.CancelFunc

func TestDaemon(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Daemon Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.Background())

	By("bootstrapping test environment")

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "hack", "crds")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.30.0-%s-%s", goruntime.GOOS, goruntime.GOARCH)),
	}

	// cfg is defined in this file globally.
	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// Create scheme and add NetworkAttachmentDefinition types
	utilruntime.Must(clientgoscheme.AddToScheme(scheme.Scheme))
	utilruntime.Must(netapi.AddToScheme(scheme.Scheme))

	kubernetesClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(kubernetesClient).NotTo(BeNil())

	// Create NetworkAttachmentDefinition client
	netClient, err = netclientset.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())

	// Create necessary namespaces
	testNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-namespace"}}
	Expect(kubernetesClient.Create(ctx, testNs)).To(Succeed())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()

	if testEnv != nil {
		err := (func() (err error) {
			// Need to sleep if the first stop fails due to a bug:
			// https://github.com/kubernetes-sigs/controller-runtime/issues/1571
			sleepTime := 1 * time.Millisecond
			for i := 0; i < 12; i++ { // Exponentially sleep up to ~4s
				if err = testEnv.Stop(); err == nil {
					return
				}
				sleepTime *= 2
				time.Sleep(sleepTime)
			}
			return
		})()
		Expect(err).NotTo(HaveOccurred())
	}
})
