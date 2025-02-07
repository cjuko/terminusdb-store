//! Common data structures and traits for all layer types.
use std::collections::HashMap;
use std::hash::Hash;

use crate::structure::{TdbDataType, TypedDictEntry};

/// A layer containing dictionary entries and triples.
///
/// A layer can be queried. To answer queries, layers will check their
/// own data structures, and if they have a parent, the parent is
/// queried as well.
pub trait Layer: Send + Sync {
    /// The name of this layer.
    fn name(&self) -> [u32; 5];
    fn parent_name(&self) -> Option<[u32; 5]>;

    /// The amount of nodes and values known to this layer.
    /// This also counts entries in the parent.
    fn node_and_value_count(&self) -> usize;
    /// The amount of predicates known to this layer.
    /// This also counts entries in the parent.
    fn predicate_count(&self) -> usize;

    /// The numerical id of a subject, or None if the subject cannot be found.
    fn subject_id(&self, subject: &str) -> Option<u64>;
    /// The numerical id of a predicate, or None if the predicate cannot be found.
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    /// The numerical id of a node object, or None if the node object cannot be found.
    fn object_node_id(&self, object: &str) -> Option<u64>;
    /// The numerical id of a value object, or None if the value object cannot be found.
    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64>;
    /// The subject corresponding to a numerical id, or None if it cannot be found.
    fn id_subject(&self, id: u64) -> Option<String>;

    /// The predicate corresponding to a numerical id, or None if it cannot be found.
    fn id_predicate(&self, id: u64) -> Option<String>;
    /// The object corresponding to a numerical id, or None if it cannot be found.
    fn id_object(&self, id: u64) -> Option<ObjectType>;

    /// The object node corresponding to a numerical id, or None if it cannot be found. Panics if the object is actually a value.
    fn id_object_node(&self, id: u64) -> Option<String> {
        self.id_object(id).map(|o| {
            o.node()
                .expect("Expected ObjectType to be node but got a value")
        })
    }

    /// The object value corresponding to a numerical id, or None if it cannot be found. Panics if the object is actually a node.
    fn id_object_value(&self, id: u64) -> Option<TypedDictEntry> {
        self.id_object(id).map(|o| {
            o.value()
                .expect("Expected ObjectType to be value but got a node")
        })
    }

    /// Check if the given id refers to a node.
    ///
    /// This will return None if the id doesn't refer to anything.
    fn id_object_is_node(&self, id: u64) -> Option<bool>;

    /// Check if the given id refers to a value.
    ///
    /// This will return None if the id doesn't refer to anything.
    fn id_object_is_value(&self, id: u64) -> Option<bool> {
        self.id_object_is_node(id).map(|v| !v)
    }

    /// Create a struct with all the counts
    fn all_counts(&self) -> LayerCounts;

    /// Return a clone of this layer in a box.
    fn clone_boxed(&self) -> Box<dyn Layer>;

    /// Returns true if the given triple exists, and false otherwise.
    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool;

    /// Returns true if the given triple exists, and false otherwise.
    fn id_triple_exists(&self, triple: IdTriple) -> bool {
        self.triple_exists(triple.subject, triple.predicate, triple.object)
    }

    /// Returns true if the given triple exists, and false otherwise.
    fn value_triple_exists(&self, triple: &ValueTriple) -> bool {
        self.value_triple_to_id(triple)
            .map(|t| self.id_triple_exists(t))
            .unwrap_or(false)
    }

    /// Iterator over all triples known to this layer.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send>;

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send>;
    fn triples_sp(&self, subject: u64, predicate: u64)
        -> Box<dyn Iterator<Item = IdTriple> + Send>;

    /// Convert a `ValueTriple` to an `IdTriple`, returning None if any of the strings in the triple could not be resolved.
    fn value_triple_to_id(&self, triple: &ValueTriple) -> Option<IdTriple> {
        self.subject_id(&triple.subject).and_then(|subject| {
            self.predicate_id(&triple.predicate).and_then(|predicate| {
                match &triple.object {
                    ObjectType::Node(node) => self.object_node_id(&node),
                    ObjectType::Value(value) => self.object_value_id(&value),
                }
                .map(|object| IdTriple {
                    subject,
                    predicate,
                    object,
                })
            })
        })
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send>;

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send>;

    /// Convert all known strings in the given string triple to ids.
    fn value_triple_to_partially_resolved(&self, triple: ValueTriple) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: self
                .subject_id(&triple.subject)
                .map(PossiblyResolved::Resolved)
                .unwrap_or(PossiblyResolved::Unresolved(triple.subject)),
            predicate: self
                .predicate_id(&triple.predicate)
                .map(PossiblyResolved::Resolved)
                .unwrap_or(PossiblyResolved::Unresolved(triple.predicate)),
            object: match &triple.object {
                ObjectType::Node(node) => self
                    .object_node_id(&node)
                    .map(PossiblyResolved::Resolved)
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object)),
                ObjectType::Value(value) => self
                    .object_value_id(&value)
                    .map(PossiblyResolved::Resolved)
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object)),
            },
        }
    }

    /// Convert an id triple to the corresponding string version, returning None if any of those ids could not be converted.
    fn id_triple_to_string(&self, triple: &IdTriple) -> Option<ValueTriple> {
        self.id_subject(triple.subject).and_then(|subject| {
            self.id_predicate(triple.predicate).and_then(|predicate| {
                self.id_object(triple.object).map(|object| ValueTriple {
                    subject,
                    predicate,
                    object,
                })
            })
        })
    }

    /// Returns the total amount of triple additions in this layer and all its parents.
    fn triple_addition_count(&self) -> usize;

    /// Returns the total amount of triple removals in this layer and all its parents.
    fn triple_removal_count(&self) -> usize;

    /// Returns the total amount of triples in this layer and all its parents.
    fn triple_count(&self) -> usize {
        self.triple_addition_count() - self.triple_removal_count()
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple>;
}

pub struct LayerCounts {
    pub node_count: usize,
    pub predicate_count: usize,
    pub value_count: usize,
}

/// A triple, stored as numerical ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
}

impl IdTriple {
    /// Construct a new id triple.
    pub fn new(subject: u64, predicate: u64, object: u64) -> Self {
        IdTriple {
            subject,
            predicate,
            object,
        }
    }

    /// convert this triple into a `PartiallyResolvedTriple`, which is a data structure used in layer building.
    pub fn to_resolved(&self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Resolved(self.subject),
            predicate: PossiblyResolved::Resolved(self.predicate),
            object: PossiblyResolved::Resolved(self.object),
        }
    }
}

/// A triple stored as strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueTriple {
    pub subject: String,
    pub predicate: String,
    pub object: ObjectType,
}

impl ValueTriple {
    /// Construct a triple with a node object.
    ///
    /// Nodes may appear in both the subject and object position.
    pub fn new_node(subject: &str, predicate: &str, object: &str) -> ValueTriple {
        ValueTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Node(object.to_owned()),
        }
    }

    /// Construct a triple with a value object.
    ///
    /// Values may only appear in the object position.
    pub fn new_value(subject: &str, predicate: &str, object: TypedDictEntry) -> ValueTriple {
        ValueTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Value(object),
        }
    }

    /// Construct a triple with a string value object.
    ///
    /// Values may only appear in the object position.
    pub fn new_string_value(subject: &str, predicate: &str, object: &str) -> ValueTriple {
        ValueTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Value(String::make_entry(&object)),
        }
    }

    /// Convert this triple to a `PartiallyResolvedTriple`, marking each field as unresolved.
    pub fn to_unresolved(self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Unresolved(self.subject),
            predicate: PossiblyResolved::Unresolved(self.predicate),
            object: PossiblyResolved::Unresolved(self.object),
        }
    }
}

/// Either a resolved id or an unresolved inner type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PossiblyResolved<T: Clone + PartialEq + Eq + PartialOrd + Ord + Hash> {
    Unresolved(T),
    Resolved(u64),
}

impl<T: Clone + PartialEq + Eq + PartialOrd + Ord + Hash> PossiblyResolved<T> {
    /// Returns true if this is a resolved id, and false otherwise.
    pub fn is_resolved(&self) -> bool {
        match self {
            Self::Unresolved(_) => false,
            Self::Resolved(_) => true,
        }
    }

    /// Return a PossiblyResolved with the inner value as a reference.
    pub fn as_ref(&self) -> PossiblyResolved<&T> {
        match self {
            Self::Unresolved(u) => PossiblyResolved::Unresolved(&u),
            Self::Resolved(id) => PossiblyResolved::Resolved(*id),
        }
    }

    /// Unwrap to the unresolved inner value, or panic if this was actually a resolved id.
    pub fn unwrap_unresolved(self) -> T {
        match self {
            Self::Unresolved(u) => u,
            Self::Resolved(_) => panic!("tried to unwrap unresolved, but got a resolved"),
        }
    }

    /// Unwrap to the resolved id, or panic if this was actually an unresolved value.
    pub fn unwrap_resolved(self) -> u64 {
        match self {
            Self::Unresolved(_) => panic!("tried to unwrap resolved, but got an unresolved"),
            Self::Resolved(id) => id,
        }
    }
}

/// A triple where the subject, predicate and object can all either be fully resolved to an id, or unresolved.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartiallyResolvedTriple {
    pub subject: PossiblyResolved<String>,
    pub predicate: PossiblyResolved<String>,
    pub object: PossiblyResolved<ObjectType>,
}

impl PartiallyResolvedTriple {
    /// Resolve the unresolved ids in this triple using the given hashmaps for nodes, predicates and values.
    pub fn resolve_with(
        &self,
        node_map: &HashMap<String, u64>,
        predicate_map: &HashMap<String, u64>,
        value_map: &HashMap<TypedDictEntry, u64>,
    ) -> Option<IdTriple> {
        let subject = match self.subject.as_ref() {
            PossiblyResolved::Unresolved(s) => *node_map.get(s)?,
            PossiblyResolved::Resolved(id) => id,
        };
        let predicate = match self.predicate.as_ref() {
            PossiblyResolved::Unresolved(p) => *predicate_map.get(p)?,
            PossiblyResolved::Resolved(id) => id,
        };
        let object = match self.object.as_ref() {
            PossiblyResolved::Unresolved(ObjectType::Node(n)) => *node_map.get(n)?,
            PossiblyResolved::Unresolved(ObjectType::Value(v)) => *value_map.get(v)?,
            PossiblyResolved::Resolved(id) => id,
        };

        Some(IdTriple {
            subject,
            predicate,
            object,
        })
    }

    pub fn as_resolved(&self) -> Option<IdTriple> {
        if !self.subject.is_resolved()
            || !self.predicate.is_resolved()
            || !self.object.is_resolved()
        {
            None
        } else {
            Some(IdTriple::new(
                self.subject.as_ref().unwrap_resolved(),
                self.predicate.as_ref().unwrap_resolved(),
                self.object.as_ref().unwrap_resolved(),
            ))
        }
    }

    pub fn make_resolved_or_zero(&mut self) {
        if !self.subject.is_resolved()
            || !self.predicate.is_resolved()
            || !self.object.is_resolved()
        {
            self.subject = PossiblyResolved::Resolved(0);
            self.predicate = PossiblyResolved::Resolved(0);
            self.object = PossiblyResolved::Resolved(0);
        }
    }
}

/// The type of an object in a triple.
///
/// Objects in a triple may either be a node or a value. Nodes can be
/// used both in the subject and the object position, while values are
/// only used in the object position.
///
/// Terminus-store keeps track of whether an object was stored as a
/// node or a value, and will return this information in queries. It
/// is possible to have the same string appear both as a node and a
/// value, without this leading to conflicts.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum ObjectType {
    Node(String),
    Value(TypedDictEntry),
}

impl ObjectType {
    pub fn node(self) -> Option<String> {
        match self {
            ObjectType::Node(s) => Some(s),
            ObjectType::Value(_) => None,
        }
    }

    pub fn node_ref(&self) -> Option<&str> {
        match self {
            ObjectType::Node(s) => Some(s),
            ObjectType::Value(_) => None,
        }
    }

    pub fn value(self) -> Option<TypedDictEntry> {
        match self {
            ObjectType::Node(_) => None,
            ObjectType::Value(v) => Some(v),
        }
    }

    pub fn value_ref(&self) -> Option<&TypedDictEntry> {
        match self {
            ObjectType::Node(_) => None,
            ObjectType::Value(v) => Some(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::internal::base::tests::base_layer_files;
    use crate::layer::internal::base::BaseLayer;
    use crate::layer::internal::child::tests::child_layer_files;
    use crate::layer::internal::child::ChildLayer;
    use crate::layer::internal::InternalLayer;
    use crate::layer::simple_builder::{LayerBuilder, SimpleLayerBuilder};
    use std::sync::Arc;

    #[tokio::test]
    async fn find_triple_after_adjacent_removal() {
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "sniff"));

        builder.commit().await.unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .await
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.commit().await.unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base.clone(), &files)
                .await
                .unwrap()
                .into(),
        );

        // TODO why are we not using these results?
        let _base_triples_additions: Vec<_> = base
            .internal_triple_additions()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        let _triples_additions: Vec<_> = child
            .internal_triple_additions()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        let _triples_removals: Vec<_> = child
            .internal_triple_removals()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        let triples: Vec<_> = child
            .triples()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            vec![ValueTriple::new_string_value("cow", "says", "sniff")],
            triples
        );
    }

    #[tokio::test]
    async fn find_triple_after_removal_and_readdition() {
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));

        builder.commit().await.unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .await
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.commit().await.unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base, &files)
                .await
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 2], child.clone(), files.clone());
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.commit().await.unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 2], child, &files)
                .await
                .unwrap()
                .into(),
        );

        let triples: Vec<_> = child
            .triples()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            vec![ValueTriple::new_string_value("cow", "says", "moo")],
            triples
        );
    }

    #[tokio::test]
    async fn find_single_triple_sp() {
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "neigh"));

        builder.commit().await.unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .await
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "neigh"));
        builder.commit().await.unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base, &files)
                .await
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 2], child.clone(), files.clone());
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.commit().await.unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 2], child, &files)
                .await
                .unwrap()
                .into(),
        );

        let id_triple_1 = child
            .single_triple_sp(
                child.subject_id("cow").unwrap(),
                child.predicate_id("says").unwrap(),
            )
            .unwrap();
        let triple_1 = child.id_triple_to_string(&id_triple_1).unwrap();

        let id_triple_2 = child
            .single_triple_sp(
                child.subject_id("duck").unwrap(),
                child.predicate_id("says").unwrap(),
            )
            .unwrap();
        let triple_2 = child.id_triple_to_string(&id_triple_2).unwrap();

        assert_eq!(
            ValueTriple::new_string_value("cow", "says", "moo"),
            triple_1
        );
        assert_eq!(
            ValueTriple::new_string_value("duck", "says", "quack"),
            triple_2
        );
    }

    #[tokio::test]
    async fn find_nonstring_triples() {
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_value_triple(ValueTriple::new_value(
            "duck",
            "num_feet",
            u32::make_entry(&2),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "cow",
            "num_feet",
            u32::make_entry(&4),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "disabled_cow",
            "num_feet",
            u32::make_entry(&3),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "duck",
            "swims",
            String::make_entry(&"true"),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "cow",
            "swims",
            String::make_entry(&"false"),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "disabled_cow",
            "swims",
            String::make_entry(&"false"),
        ));

        builder.commit().await.unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .await
                .unwrap()
                .into(),
        );

        let mut results: Vec<_> = base
            .triples_p(base.predicate_id("num_feet").unwrap())
            .map(|t| {
                (
                    base.id_subject(t.subject).unwrap(),
                    base.id_object_value(t.object).unwrap().as_val::<u32, u32>(),
                )
            })
            .collect();
        results.sort();

        let expected = vec![
            ("cow".to_owned(), 4),
            ("disabled_cow".to_owned(), 3),
            ("duck".to_owned(), 2),
        ];

        assert_eq!(expected, results);
    }
}
