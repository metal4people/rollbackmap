#![cfg(test)]

use crate::rollbackmap::RollbackMap;
use std::convert::TryFrom;

#[test]
fn test_insert() {
    // insert the new key
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        let updated = map.insert(1, "one");
        assert_eq!(updated, None);
    }
    // insert value with existing key (update the value)
    {
        let count: usize = 101;
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        map.insert(1, "1".to_owned());

        for n in 2..count {
            let updated = map.insert(1, n.to_string());
            assert_eq!(updated.unwrap(), (n - 1).to_string());
        }
    }
    // insert existing value from previous checkpoint is returned
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        let mut updated = map.insert(1, "p01");
        assert_eq!(updated, None);
        map.insert(2, "p02");
        map.checkpoint();
        updated = map.insert(1, "p11");
        assert_eq!(updated, Some("p01"));
        map.checkpoint();
        updated = map.insert(1, "p21");
        assert_eq!(updated, Some("p11"));
        updated = map.insert(2, "p22");
        assert_eq!(updated, Some("p02"));
    }
}

#[test]
fn test_len() {
    // Empty + one element
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        assert_eq!(map.len(), 0);
        map.insert(1, "a");
        assert_eq!(map.len(), 1);
    }
    // Loop insert + remove in one checkpoint
    {
        let count: usize = 101;
        let mut map: RollbackMap<usize, &str> = RollbackMap::new();
        for n in 1..count {
            map.insert(n, "a");
            map.remove(&n);
            map.insert(n, "a");
            assert_eq!(map.len(), n);
        }
        assert_eq!(map.len(), count - 1);
        map.clear();
        assert_eq!(map.len(), 0);
    }
    // Loop insert + double removes in one checkpoint
    {
        let count: usize = 101;
        let mut map: RollbackMap<usize, &str> = RollbackMap::new();
        for n in 1..count {
            map.insert(n, "a");
            map.insert(count + n, "a");
            assert_eq!(map.len(), n + 1);
            map.remove(&n);
            assert_eq!(map.len(), n);
            map.remove(&n);
            assert_eq!(map.len(), n);
        }
        assert_eq!(map.len(), count - 1);
        map.clear();
        assert_eq!(map.len(), 0);
    }
    // Loop double insert + removes in one checkpoint
    {
        let count: usize = 101;
        let mut map: RollbackMap<usize, &str> = RollbackMap::new();
        for n in 1..count {
            map.insert(n, "a");
            map.insert(n, "a");
            assert_eq!(map.len(), 1);
            map.remove(&n);
        }
        assert_eq!(map.len(), 0);
    }
    // Insert + remove via multiple checkpoints
    {
        let count: usize = 101;
        let mut map: RollbackMap<usize, &str> = RollbackMap::new();
        for n in 1..count {
            map.insert(n, "a");
            map.checkpoint();
            map.remove(&n);
        }
        assert_eq!(map.len(), 0);
    }
    // Insert + remove + rollback via multiple checkpoints
    {
        let count: usize = 101;
        let mut map: RollbackMap<usize, &str> = RollbackMap::new();
        for n in 1..count {
            map.insert(n, "a");
            let checkpoint = map.checkpoint();
            map.remove(&n);
            map.rollback(checkpoint.unwrap());
        }
        assert_eq!(map.len(), count - 1);
    }
}

#[test]
fn test_remove() {
    // remove from the empty map
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        let updated = map.remove(&1);
        assert_eq!(updated, None);
    }
    // remove just added value map
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        map.insert(1, "one");
        assert_eq!(map.remove(&1), Some("one"));
    }
    // remove for the set of keys
    {
        let count: u32 = 101;
        let mut map: RollbackMap<u32, String> = RollbackMap::new();

        for n in 1..count {
            map.insert(n, n.to_string());
        }

        for n in 1..count {
            let removed_value = map.remove(&n);
            assert_eq!(removed_value.unwrap(), n.to_string());
        }
    }
    // remove among checkpoints
    {
        let count: u32 = 101;
        let mut map: RollbackMap<u32, String> = RollbackMap::new();

        for n in 1..count {
            map.insert(n, n.to_string());
            map.checkpoint();
        }

        for n in 1..count {
            let removed_value = map.remove(&n);
            assert_eq!(removed_value.unwrap(), n.to_string());
        }

        for n in 1..count {
            assert_eq!(false, map.contains_key(&n));
            let removed_value = map.remove(&n);
            assert_eq!(removed_value, None);
        }
    }
}

#[test]
fn test_contains_key() {
    // insert the new key
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        map.insert(1, "one");
        assert!(map.contains_key(&1));
    }
    // Checkpoints sequence
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        let count: u32 = 101;
        {
            for n in 1..count {
                map.insert(n, 1.to_string());
                map.checkpoint();
                assert_eq!(usize::try_from(n).unwrap(), map.get_checkpoints_count());
            }
            for n in 1..count {
                assert!(map.contains_key(&n));
            }
        }
    }
}

#[test]
fn test_get() {
    // empty map
    {
        let map: RollbackMap<u32, &str> = RollbackMap::new();
        assert_eq!(None, map.get(&1));
    }
    // insert + update
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        map.insert(1, "p01");
        assert_eq!(map.get(&1), Some(&"p01"));
        map.insert(1, "p11");
        assert_eq!(map.get(&1), Some(&"p11"));
    }
    // insert + update + checkpoints + remove + rollback
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        map.insert(1, "p01");
        map.insert(2, "p02");
        assert_eq!(map.get(&1), Some(&"p01"));
        assert_eq!(map.get(&2), Some(&"p02"));
        map.checkpoint();
        map.insert(1, "p11");
        assert_eq!(map.get(&1), Some(&"p11"));
        assert_eq!(map.get(&2), Some(&"p02"));
        map.checkpoint();
        map.remove(&2);
        assert_eq!(map.get(&2), None);
        map.rollback(map.get_last_checkpoint().unwrap());
        assert_eq!(map.get(&2), Some(&"p02"));
    }
}

#[test]
fn test_clear() {
    // insert + checkpoints + clear
    {
        let mut map: RollbackMap<u32, &str> = RollbackMap::new();
        map.insert(10, "p01");
        map.checkpoint();
        map.insert(11, "p11");
        map.checkpoint();
        map.clear();
        assert_eq!(map.contains_key(&10), false);
        assert_eq!(map.contains_key(&11), false);
    }
}

#[test]
fn test_checkpoint() {
    // Using returned value from checkpoint
    {
        let mut map = RollbackMap::new();
        assert_eq!(map.get_last_checkpoint(), None);
        map.insert(1, "a");
        map.insert(2, "b");
        let checkpoint = map.checkpoint();
        assert!(checkpoint.is_some());
        map.insert(1, "xa");
        map.remove(&2);
        assert!(map.rollback(checkpoint.unwrap()));
        assert_eq!(map.get(&1), Some(&"a"));
        assert_eq!(map.get(&2), Some(&"b"));
    }
    // Use get last checkpoint for rollback
    {
        let mut map = RollbackMap::new();
        assert_eq!(map.get_last_checkpoint(), None);
        map.insert(1, "a");
        map.insert(2, "b");
        assert!(map.checkpoint().is_some());
        map.insert(1, "xa");
        map.remove(&2);
        assert!(map.rollback(map.get_last_checkpoint().unwrap()));
        assert_eq!(map.get(&1), Some(&"a"));
        assert_eq!(map.get(&2), Some(&"b"));
    }
}

#[test]
fn test_get_last_checkpoint() {
    // Empty map
    {
        let map: RollbackMap<u32, &str> = RollbackMap::new();
        let last_checkpoint = map.get_last_checkpoint();
        assert_eq!(last_checkpoint, None);
    }
    // Checkpoints sequence
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        let count: usize = 101;
        for n in 1..count {
            let last_checkpoint = map.checkpoint();
            assert_eq!(last_checkpoint, map.get_last_checkpoint());
            assert_eq!(n, map.get_checkpoints_count());
        }

        for n in 1..count {
            if let Some(prev_checkpoint) = map.get_prev_checkpoint() {
                map.rollback(prev_checkpoint);
                assert_eq!(Some(prev_checkpoint), map.get_last_checkpoint());
                assert_eq!(count - n - 1, map.get_checkpoints_count());
            }
        }
    }
    // Checkpoints sequence with rollback
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        let count = 100;
        for _n in 1..count {
            let last_checkpoint = map.checkpoint();
            assert_eq!(last_checkpoint, map.get_last_checkpoint());
        }

        for _n in 1..count {
            if let Some(prev_checkpoint) = map.get_prev_checkpoint() {
                map.rollback(prev_checkpoint);
                assert_eq!(Some(prev_checkpoint), map.get_last_checkpoint());
            }
        }
    }
}

#[test]
fn test_get_prev_checkpoint() {
    let mut map = RollbackMap::new();
    assert_eq!(map.get_last_checkpoint(), None);
    map.insert(1, "a");
    map.insert(2, "b");
    let first_checkpoint = map.checkpoint();
    assert!(first_checkpoint.is_some());
    assert_eq!(map.get_prev_checkpoint(), None);
    map.insert(1, "xa");
    map.remove(&2);
    let second_checkpoint = map.checkpoint();
    assert!(second_checkpoint.is_some());
    map.insert(1, "xb");
    map.insert(2, "xc");
    assert_eq!(map.get(&1), Some(&"xb"));
    assert_eq!(map.get(&2), Some(&"xc"));
    assert_eq!(map.get_prev_checkpoint(), first_checkpoint);
    assert!(map.rollback(first_checkpoint.unwrap()));
    assert_eq!(map.get(&1), Some(&"a"));
    assert_eq!(map.get(&2), Some(&"b"));
    assert_eq!(false, map.rollback(second_checkpoint.unwrap()));
}

#[test]
fn test_rollback() {
    // Rollback inserts
    {
        let count = 101;
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        map.insert(1, "0".to_owned());

        for n in 1..count {
            map.checkpoint();
            map.insert(1, n.to_string());
        }
        {
            let value = map.get(&1);
            assert_eq!(*value.unwrap(), 100.to_string());
            assert!(map.rollback(map.get_last_checkpoint().unwrap()));
        }
        for n in 2..count {
            let value = map.get(&1);
            assert_eq!(*value.unwrap(), (count - n).to_string());
            if let Some(prev_checkpoint) = map.get_prev_checkpoint() {
                assert!(map.rollback(prev_checkpoint));
            }
        }
    }
    // Rollback insert + removes
    {
        let count = 101;
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        map.insert(1, "0".to_owned());

        for n in 1..count {
            map.insert(n, n.to_string());
        }
        for n in 1..count {
            map.checkpoint();
            map.remove(&n);
        }

        {
            assert!(map.rollback(map.get_last_checkpoint().unwrap()));
            let value = map.get(&100).unwrap().clone();
            assert_eq!(value, 100.to_string());
        }

        for n in 2..count {
            for j in 1..count - n {
                assert_eq!(map.contains_key(&j), false);
            }
            for j in 1..n {
                assert_eq!(*map.get(&(count - j)).unwrap(), (count - j).to_string());
            }
            assert!(map.rollback(map.get_prev_checkpoint().unwrap()));
        }
    }

    // Rollback non-valid checkpoint
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        map.insert(1, "0".to_owned());
        map.checkpoint();
        let non_valid_checkkpoint = map.get_last_checkpoint().unwrap() + 1;
        assert_ne!(true, map.rollback(non_valid_checkkpoint));
    }
}

#[test]
fn test_prune() {
    // Empty map
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        let last_checkpoint = map.prune();
        assert_eq!(last_checkpoint, None);
    }
    // Checkpoints sequence
    {
        let mut map: RollbackMap<u32, String> = RollbackMap::new();
        let count: usize = 101;
        {
            let mut last_checkpoint: Option<u32> = None;
            for _n in 1..count {
                last_checkpoint = map.checkpoint();
            }
            assert_eq!(last_checkpoint, map.prune());
            assert_eq!(1, map.get_checkpoints_count());
        }
        {
            for _n in 1..count {
                let last_checkpoint = map.checkpoint();
                assert_eq!(last_checkpoint, map.prune());
                assert_eq!(1, map.get_checkpoints_count());
            }
        }
    }
    {
        let mut map = RollbackMap::new();
        assert_eq!(map.get_last_checkpoint(), None);
        map.insert(1, "a");
        map.insert(2, "b");
        let first_checkpoint = map.checkpoint();
        assert!(first_checkpoint.is_some());
        assert_eq!(map.get_prev_checkpoint(), None);
        map.insert(1, "xa");
        map.remove(&2);
        let second_checkpoint = map.checkpoint();
        assert!(second_checkpoint.is_some());
        map.insert(1, "xb");
        map.insert(2, "xc");
        assert_eq!(map.get(&1), Some(&"xb"));
        assert_eq!(map.get(&2), Some(&"xc"));
        assert_eq!(map.prune(), second_checkpoint);
        assert_eq!(map.get(&1), Some(&"xb"));
        assert_eq!(map.get(&2), Some(&"xc"));
        assert_eq!(false, map.rollback(first_checkpoint.unwrap()));
        assert_eq!(true, map.rollback(second_checkpoint.unwrap()));
        assert_eq!(map.get(&1), Some(&"xa"));
        assert_eq!(map.get(&2), None);
    }
}
