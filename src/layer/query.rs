use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::iter::repeat;
use std::sync::Arc;
use canrun::{Value, all, unify, StateIter, State, Fork};
use canrun::goals::Goal;
use canrun::Value::{Var, Resolved};

use crate::Layer;
use crate::layer::InternalLayer;

#[derive(Clone)]
struct TripleQueryGoal {
    s: Value<u64>,
    p: Value<u64>,
    o: Value<u64>,
    layer: Arc<InternalLayer>
}

impl TripleQueryGoal {
    fn new(layer: Arc<InternalLayer>, s: Value<u64>, p: Value<u64>, o: Value<u64>) -> Self {
        Self { s, p, o, layer }
    }
}


impl Debug for TripleQueryGoal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Goal for TripleQueryGoal {
    fn apply(&self, state: State) -> Option<State> {
        state.fork(self.clone())
    }
}

impl Fork for TripleQueryGoal {

    fn fork(&self, state: &State) -> StateIter {
        let states = repeat(state.clone());

        let query_s = state.resolve(&self.s);
        let query_p = state.resolve(&self.p);
        let query_o = state.resolve(&self.o);

        match (query_s, query_p, query_o) {
            (Var(s), Var(p), Var(o)) => {
                let goals = self.layer.triples();
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        all![unify(s, Value::new(g.subject)),
                             unify(p, Value::new(g.predicate)),
                             unify(o, Value::new(g.object))].apply(state).into_iter()))
            },
            (Var(s), Var(p), Resolved(o)) => {
                let goals = self.layer.triples_o(*o);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        all![unify(s, Value::new(g.subject)),
                             unify(p, Value::new(g.predicate))].apply(state).into_iter()))
            },
            (Var(s), Resolved(p), Var(o)) => {
                let goals = self.layer.triples_p(*p);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        all![unify(s, Value::new(g.subject)),
                             unify(o, Value::new(g.object))].apply(state).into_iter()))
            },
            (Var(s), Resolved(p), Resolved(o)) => {
                let goals =
                    self.layer.triples_o(*o).into_iter().filter(move |g| g.predicate == *p);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        unify(s, Value::new(g.subject)).apply(state).into_iter()))
            },
            (Resolved(s), Var(p), Var(o)) => {
                let goals = self.layer.triples_s(*s);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        all![unify(p, Value::new(g.predicate)),
                             unify(o, Value::new(g.object))].apply(state).into_iter()))
            },
            (Resolved(s), Var(p), Resolved(o)) => {
                let goals = self.layer.triples_o(*o).into_iter().filter(move |g| g.subject == *s);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        unify(p, Value::new(g.predicate)).apply(state).into_iter()))
            },
            (Resolved(s), Resolved(p), Var(o)) => {
                let goals = self.layer.triples_sp(*s, *p);
                Box::new(goals.zip(states).flat_map(
                    move |(g, state)|
                        unify(o, Value::new(g.object)).apply(state).into_iter()))
            },
            (Resolved(s), Resolved(p), Resolved(o)) => {
                let res = self.layer.triple_exists(*s, *p, *o);
                if res {
                    Box::new(Some(state.clone()).into_iter())
                } else {
                    Box::new(None.into_iter())
                }
            }
        }
    }
}

fn query(layer: InternalLayer, sparql_select: Vec<String>, sparql_where: Vec<[String; 3]>) {
    let unique_vars: HashSet<&String> = sparql_where.iter().flatten().filter(|x| x.starts_with("?")).collect();
    let vars: HashMap<String, Value<u64>> = unique_vars.into_iter().map(|v| (v.clone(), Value::var())).collect();
    let t = sparql_where.iter().map(|[s, p, o]| {
        
    });
}

#[cfg(test)]
mod tests {
    use canrun::{all, Query, Value};
    use crate::layer::base::tests::{base_layer_files};
    use crate::layer::query::TripleQueryGoal;
    use std::sync::Arc;
    use crate::layer::{BaseLayer, InternalLayer, LayerBuilder, SimpleLayerBuilder};
    use crate::structure::TdbDataType;
    use crate::ValueTriple;

    #[tokio::test]
    async fn matching_fails_if_any_further_goals_fail() {
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());
        builder.add_value_triple(ValueTriple::new_value(
            "duck",
            "num_feet",
            u32::make_entry(&2),
        ));
        builder.add_value_triple(ValueTriple::new_value(
            "dog",
            "num_feet",
            u32::make_entry(&4),
        ));
        builder.commit().await.unwrap();
        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .await
                .unwrap()
                .into());

        let s = Value::var();
        let p = Value::var();
        let o = Value::new(4);

        let g = all![TripleQueryGoal::new(base, s.clone(), p.clone(), o.clone())];

        for (_s, _p, _o) in g.query((s, p, o)) {
            println!("{:?} {:?} {:?}", _s, _p, _o);
        }
    }
}