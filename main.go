package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record/util"
	"k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
)

const (
	RecordPeriod        = 1 * time.Second
	RecordCount         = 100 //
	QueueLength         = 100
	FullChannelBehavior = watch.DropIfChannelFull
	Namespace           = "keg"
	ResourceName        = "keg"
	ReportingController = "kubernetes-event-generator"
	EventReason         = "ProactiveGenerate"
	EventType           = "Normal"
	EventAction         = "No"
)

func main() {
	// Initialize klog flags and ensure logs are flushed on exit
	klog.InitFlags(flag.CommandLine)
	defer klog.Flush()

	// Setup kubeconfig path from home directory or command line flag
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// Initialize Kubernetes client configuration
	var (
		config *rest.Config
		err    error
	)
	if len(*kubeconfig) == 0 {
		config, err = rest.InClusterConfig()
		if err != nil {
			klog.Errorf("in cluster config, err: %v", err)
			return
		}
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			klog.Errorf("build config from flags, err: %v", err)
			return
		}
	}

	// Create Kubernetes clientset
	config.QPS = -1 // disable client-side ratelimiting
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("new for config, err: %v", err)
		return
	}

	// Initialize event broadcaster and watcher
	broadcaster := watch.NewBroadcaster(QueueLength, FullChannelBehavior)
	watcher, err := broadcaster.Watch()
	if err != nil {
		klog.Errorf("watch broadcaster, err: %v", err)
		return
	}

	// Setup signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	go func() {
		var (
			ns  *corev1.Namespace
			cm  *corev1.ConfigMap
			err error
		)
		// Create namespace if not exists
		ns, err = clientset.CoreV1().Namespaces().Get(ctx, Namespace, metav1.GetOptions{})
		if err != nil {
			if !errors.IsAlreadyExists(err) {
				klog.Error("create namespace", "err", err)
				return
			}
			ns, err = clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: Namespace}}, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("get namespace, err: %v", err)
				return
			}
		}

		// Create ConfigMap if not exists
		cm, err = clientset.CoreV1().ConfigMaps(ns.GetName()).Get(ctx, ResourceName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsAlreadyExists(err) {
				klog.Error("create configmap", "err", err)
				return
			}
			cm, err = clientset.CoreV1().ConfigMaps(ns.GetName()).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name: ResourceName,
				},
			}, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("get configmap, err: %v", err)
				return
			}
		}

		scheme := k8sruntime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		ref, err := reference.GetReference(scheme, cm)
		if err != nil {
			klog.Errorf("get configmap reference, err: %v", err)
			return
		}
		hostname, err := os.Hostname()
		if err != nil {
			klog.Errorf("get hostname, err: %v", err)
			return
		}

		// Start event generation loop
		ticker := time.NewTicker(RecordPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for range RecordCount {
					timestamp := metav1.NewMicroTime(time.Now())
					event := &eventsv1.Event{
						ObjectMeta: metav1.ObjectMeta{
							Name:      util.GenerateEventName(ref.Name, timestamp.UnixNano()),
							Namespace: cm.GetNamespace(),
						},
						EventTime:           timestamp,
						Series:              nil,
						Regarding:           *ref,
						ReportingController: ReportingController,
						ReportingInstance:   ReportingController + "-" + hostname,
						Reason:              EventReason,
						Type:                EventType,
						Action:              EventAction,
					}
					err = broadcaster.Action(watch.Added, event)
					if err != nil {
						klog.Warningf("broadcast event, err: %v", err)
						continue
					}
				}
			case <-ctx.Done():
				stop()
				watcher.Stop()
				return
			}
		}
	}()

	maxWorkers := runtime.GOMAXPROCS(0)
	sem := semaphore.NewWeighted(int64(maxWorkers))

	eventSink := events.EventSinkImpl{Interface: clientset.EventsV1()}
	for event := range watcher.ResultChan() {
		if err = sem.Acquire(ctx, 1); err != nil {
			klog.Errorf("acquire semaphore, err: %v", err)
			break
		}
		go func(obj k8sruntime.Object) {
			defer sem.Release(1)
			event, ok := obj.(*eventsv1.Event)
			if !ok {
				klog.Errorf("unexpected type, expected eventsv1.Event, got %T", obj)
				return
			}
			event, err = eventSink.Create(ctx, event)
			if err != nil {
				klog.Errorf("create event, err: %v", err)
				return
			}
			klog.Info("Event occurred", "object", klog.KRef(event.Regarding.Namespace, event.Regarding.Name), "kind", event.Regarding.Kind, "apiVersion", event.Regarding.APIVersion, "type", event.Type, "reason", event.Reason, "action", event.Action, "note", event.Note)
		}(event.Object)
	}
	err = sem.Acquire(ctx, int64(maxWorkers))
	if err != nil {
		klog.Errorf("acquire semaphore, err: %v", err)
	}
}
