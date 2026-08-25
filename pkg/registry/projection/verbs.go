package projection

import (
	"context"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
)

// The verbs a resource advertises in discovery come from which interfaces its
// storage satisfies, and Go decides that at compile time. One storage type for
// every projection therefore advertises the same verbs for all of them —
// including the ones a projection has no query for, which are then refused at
// request time with 405.
//
// That is not only untidy. The garbage collector picks resources to collect by
// looking for list, watch and delete in discovery, so it keeps issuing deletes
// that can never succeed; kubectl delete --all and apply --prune fail on a
// projection that never offered to be deleted; and an informer on a projection
// with watch.disabled lists, watches, is refused, and never syncs.
//
// So the storage is assembled from the verbs a projection actually declares.
// Each verb is a small type carrying one method, and the combinations below are
// the twenty-four ways they go together — one line each, because the methods
// come from the parts. Nothing here decides behaviour; the implementations stay
// on REST and WritableREST, which is also what the tests exercise directly.

// readable is what every projection can do: it is the storage minus the verbs a
// projection may not have.
//
// Spelled out rather than embedding *REST, because embedding would promote
// Watch to every combination and put back the thing this exists to fix.
type readable struct{ r *REST }

func (s readable) New() runtime.Object     { return s.r.New() }
func (s readable) NewList() runtime.Object { return s.r.NewList() }
func (s readable) Destroy()                { s.r.Destroy() }
func (s readable) NamespaceScoped() bool   { return s.r.NamespaceScoped() }
func (s readable) GetSingularName() string { return s.r.GetSingularName() }
func (s readable) ShortNames() []string    { return s.r.ShortNames() }
func (s readable) Categories() []string    { return s.r.Categories() }

func (s readable) GroupVersionKind(gv schema.GroupVersion) schema.GroupVersionKind {
	return s.r.GroupVersionKind(gv)
}

func (s readable) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.r.Get(ctx, name, options)
}

func (s readable) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return s.r.List(ctx, options)
}

func (s readable) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return s.r.ConvertToTable(ctx, object, tableOptions)
}

// watchable adds watch, for a projection that has not disabled it.
type watchable struct{ r *REST }

func (s watchable) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	return s.r.Watch(ctx, options)
}

// creatable adds create, for a projection with queries.create.
type creatable struct{ w *WritableREST }

func (s creatable) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return s.w.Create(ctx, obj, createValidation, options)
}

// updatable adds update, and with the Get above also patch.
type updatable struct{ w *WritableREST }

func (s updatable) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	return s.w.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// deletable adds delete, for a projection with queries.delete.
type deletable struct{ w *WritableREST }

func (s deletable) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return s.w.Delete(ctx, name, deleteValidation, options)
}

// collectionDeletable adds deletecollection, which a projection can serve
// through a collection statement of its own or by deleting one row at a time.
type collectionDeletable struct{ w *WritableREST }

func (s collectionDeletable) DeleteCollection(
	ctx context.Context,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
	listOptions *metainternalversion.ListOptions,
) (runtime.Object, error) {
	return s.w.DeleteCollection(ctx, deleteValidation, options, listOptions)
}

// The combinations. Read-only at the top, everything at the bottom.
type storageRead struct{ readable }
type storageK struct {
	readable
	collectionDeletable
}
type storageD struct {
	readable
	deletable
	collectionDeletable
}
type storageU struct {
	readable
	updatable
}
type storageUK struct {
	readable
	updatable
	collectionDeletable
}
type storageUD struct {
	readable
	updatable
	deletable
	collectionDeletable
}
type storageC struct {
	readable
	creatable
}
type storageCK struct {
	readable
	creatable
	collectionDeletable
}
type storageCD struct {
	readable
	creatable
	deletable
	collectionDeletable
}
type storageCU struct {
	readable
	creatable
	updatable
}
type storageCUK struct {
	readable
	creatable
	updatable
	collectionDeletable
}
type storageCUD struct {
	readable
	creatable
	updatable
	deletable
	collectionDeletable
}
type storageW struct {
	readable
	watchable
}
type storageWK struct {
	readable
	watchable
	collectionDeletable
}
type storageWD struct {
	readable
	watchable
	deletable
	collectionDeletable
}
type storageWU struct {
	readable
	watchable
	updatable
}
type storageWUK struct {
	readable
	watchable
	updatable
	collectionDeletable
}
type storageWUD struct {
	readable
	watchable
	updatable
	deletable
	collectionDeletable
}
type storageWC struct {
	readable
	watchable
	creatable
}
type storageWCK struct {
	readable
	watchable
	creatable
	collectionDeletable
}
type storageWCD struct {
	readable
	watchable
	creatable
	deletable
	collectionDeletable
}
type storageWCU struct {
	readable
	watchable
	creatable
	updatable
}
type storageWCUK struct {
	readable
	watchable
	creatable
	updatable
	collectionDeletable
}
type storageWCUD struct {
	readable
	watchable
	creatable
	updatable
	deletable
	collectionDeletable
}

// newStorage assembles the storage type whose advertised verbs match the
// queries this projection declares.
func newProjectionStorage(r *REST, w *WritableREST) rest.Storage {
	read := readable{r: r}
	canWatch := r.watch != nil

	var (
		canCreate     bool
		canUpdate     bool
		canDelete     bool
		canDeleteColl bool
	)
	if w != nil {
		canCreate = w.create != nil
		canUpdate = w.update != nil
		canDelete = w.delete != nil
		// A projection with only a collection statement can still serve
		// deletecollection; one with a row statement serves both, deleting a
		// row at a time when a single statement cannot express the request.
		canDeleteColl = w.delete != nil || w.deleteCollection != nil
	}

	watchPart := watchable{r: r}
	createPart := creatable{w: w}
	updatePart := updatable{w: w}
	deletePart := deletable{w: w}
	collPart := collectionDeletable{w: w}

	switch {
	case !canWatch && !canCreate && !canUpdate && !canDelete && !canDeleteColl:
		return &storageRead{read}
	case !canWatch && !canCreate && !canUpdate && !canDelete && canDeleteColl:
		return &storageK{read, collPart}
	case !canWatch && !canCreate && !canUpdate && canDelete:
		return &storageD{read, deletePart, collPart}
	case !canWatch && !canCreate && canUpdate && !canDelete && !canDeleteColl:
		return &storageU{read, updatePart}
	case !canWatch && !canCreate && canUpdate && !canDelete && canDeleteColl:
		return &storageUK{read, updatePart, collPart}
	case !canWatch && !canCreate && canUpdate && canDelete:
		return &storageUD{read, updatePart, deletePart, collPart}
	case !canWatch && canCreate && !canUpdate && !canDelete && !canDeleteColl:
		return &storageC{read, createPart}
	case !canWatch && canCreate && !canUpdate && !canDelete && canDeleteColl:
		return &storageCK{read, createPart, collPart}
	case !canWatch && canCreate && !canUpdate && canDelete:
		return &storageCD{read, createPart, deletePart, collPart}
	case !canWatch && canCreate && canUpdate && !canDelete && !canDeleteColl:
		return &storageCU{read, createPart, updatePart}
	case !canWatch && canCreate && canUpdate && !canDelete && canDeleteColl:
		return &storageCUK{read, createPart, updatePart, collPart}
	case !canWatch && canCreate && canUpdate && canDelete:
		return &storageCUD{read, createPart, updatePart, deletePart, collPart}
	case canWatch && !canCreate && !canUpdate && !canDelete && !canDeleteColl:
		return &storageW{read, watchPart}
	case canWatch && !canCreate && !canUpdate && !canDelete && canDeleteColl:
		return &storageWK{read, watchPart, collPart}
	case canWatch && !canCreate && !canUpdate && canDelete:
		return &storageWD{read, watchPart, deletePart, collPart}
	case canWatch && !canCreate && canUpdate && !canDelete && !canDeleteColl:
		return &storageWU{read, watchPart, updatePart}
	case canWatch && !canCreate && canUpdate && !canDelete && canDeleteColl:
		return &storageWUK{read, watchPart, updatePart, collPart}
	case canWatch && !canCreate && canUpdate && canDelete:
		return &storageWUD{read, watchPart, updatePart, deletePart, collPart}
	case canWatch && canCreate && !canUpdate && !canDelete && !canDeleteColl:
		return &storageWC{read, watchPart, createPart}
	case canWatch && canCreate && !canUpdate && !canDelete && canDeleteColl:
		return &storageWCK{read, watchPart, createPart, collPart}
	case canWatch && canCreate && !canUpdate && canDelete:
		return &storageWCD{read, watchPart, createPart, deletePart, collPart}
	case canWatch && canCreate && canUpdate && !canDelete && !canDeleteColl:
		return &storageWCU{read, watchPart, createPart, updatePart}
	case canWatch && canCreate && canUpdate && !canDelete && canDeleteColl:
		return &storageWCUK{read, watchPart, createPart, updatePart, collPart}
	case canWatch && canCreate && canUpdate && canDelete:
		return &storageWCUD{read, watchPart, createPart, updatePart, deletePart, collPart}
	}

	// Unreachable: the conditions above are exhaustive over four booleans.
	return &storageRead{read}
}

// Compile-time assertions at the two ends of the matrix: the fullest type
// advertises everything, and the read-only one is still a usable storage.
// That the read-only one does *not* satisfy the write interfaces cannot be
// asserted here — a type not implementing an interface is not a compile error —
// so TestAdvertisedVerbsMatchDeclaredQueries checks it by reflection.
var (
	_ rest.Storage                  = &storageRead{}
	_ rest.Scoper                   = &storageRead{}
	_ rest.Getter                   = &storageRead{}
	_ rest.Lister                   = &storageRead{}
	_ rest.SingularNameProvider     = &storageRead{}
	_ rest.GroupVersionKindProvider = &storageRead{}
	_ rest.ShortNamesProvider       = &storageRead{}
	_ rest.CategoriesProvider       = &storageRead{}

	_ rest.Watcher           = &storageWCUD{}
	_ rest.Creater           = &storageWCUD{}
	_ rest.Updater           = &storageWCUD{}
	_ rest.Patcher           = &storageWCUD{}
	_ rest.GracefulDeleter   = &storageWCUD{}
	_ rest.CollectionDeleter = &storageWCUD{}
)
