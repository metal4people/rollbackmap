use core::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::vec::Vec;

#[derive(Debug)]
pub struct VersionState<K, V> {
    /// Keys that are requested to be removed, but are present only in the previous versions
    pub removed_keys: BTreeSet<K>,

    /// Current version added nodes
    pub data: BTreeMap<K, V>,

    /// Is set to true when clear method is called
    pub detached: bool,

    /// Checkpoint
    pub checkpoint: u32,

    /// Count of values
    pub values_count: usize,
}

impl<K, V> VersionState<K, V>
where
    K: Ord,
{
    pub fn new(checkpoint: u32, values_count: usize) -> Self {
        VersionState {
            removed_keys: BTreeSet::new(),
            data: BTreeMap::new(),
            detached: false,
            checkpoint: checkpoint,
            values_count: values_count,
        }
    }
    pub fn reset(&mut self, values_count: usize) {
        self.removed_keys.clear();
        self.data.clear();
        self.detached = false;
        self.values_count = values_count;
    }
}

/// A map that provides rolling back functionality.
///
/// In addition to the the insert-get-remove operations, it allows to:
/// - create checkpoint;
/// - rollback (only in backward direction) to some specific checkpoint;
/// - remove all created checkpoints except the last one;

#[derive(Debug)]
pub struct RollbackMap<K, V>
where
    K: Ord,
{
    versions: Vec<VersionState<K, V>>,
}

// Implementation of basic map functions
impl<K: Ord + Clone, V: Clone> RollbackMap<K, V> {
    /// Makes a new, empty `RollbackMap`.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    ///
    /// // entries can now be inserted into the empty map
    /// map.insert(1, "a");
    /// ```
    pub fn new() -> Self {
        RollbackMap {
            versions: vec![VersionState::new(0, 0)],
        }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, `None` is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// map.insert(37, "b");
    /// assert_eq!(map.insert(37, "c"), Some("b"));
    /// assert_eq!(map.get(&37), Some(&"c"));
    /// ```
    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Ord,
    {
        let mut pv: Option<V> = None;
        if let Some(last) = self.versions.last() {
            if !last.removed_keys.contains(&key) {
                if let Some(existing) = self.deep_get_key_value(&key) {
                    pv = Some(existing.1.clone())
                }
            }
        }

        if let Some(last) = self.versions.last_mut() {
            last.removed_keys.remove(&key);
            last.data.insert(key, value);
            if pv.is_none() {
                last.values_count += 1;
            }
        }

        return pv;
    }
    /// Removes a key from the map, returning the value at the key if the key
    /// was previously in the map.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.remove(&1), Some("a"));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        if let Some(last) = self.versions.last_mut() {
            if last.data.contains_key(key) {
                last.values_count -= 1;
                return last.data.remove(key);
            }
        }

        let mut found: Option<(K, V)> = None;
        {
            let result = self.deep_get_key_value(&*key);
            if result.is_some() {
                found = Some((result.unwrap().0.clone(), result.unwrap().1.clone()));
            }
        }

        if found.is_some() {
            if let Some(last) = self.versions.last_mut() {
                last.values_count -= 1;
                last.removed_keys.insert(found.as_ref().unwrap().0.clone());
            }
            return Some(found.as_ref().unwrap().1.clone());
        }

        return None;
    }

    /// Returns `true` if the map contains a value for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form *must* match the ordering on the key type.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.contains_key(&1), true);
    /// assert_eq!(map.contains_key(&2), false);
    /// ```
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        let key_value = self.deep_get_key_value(key);
        return key_value.is_some();
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), None);
    /// ```
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        let key_value = self.deep_get_key_value(key);
        if !key_value.is_some() {
            return None;
        }

        return Some(key_value.unwrap().1);
    }

    fn deep_get_key_value<Q: ?Sized>(&self, key: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        for version in self.versions.iter().rev() {
            let key_value = version.data.get_key_value(key);
            if key_value.is_some() {
                return key_value;
            }
            if version.removed_keys.contains(key) {
                return None;
            }
            if version.detached {
                break;
            }
        }
        return None;
    }
    /// Clears data in the RollbackMap instance.
    /// Data can be restored if was saved by checkpoint call.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    ///
    /// map.insert(1, "a");
    /// map.clear()
    /// ```
    pub fn clear(&mut self) {
        if let Some(last) = self.versions.last_mut() {
            last.data.clear();
            last.removed_keys.clear();
            last.detached = true;
            last.values_count = 0;
        }
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.len(), 0);
    /// map.insert(1, "a");
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        if let Some(last) = self.versions.last() {
            return last.values_count;
        }
        return 0;
    }

    /// Returns `true` if the map contains no elements.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut a = RollbackMap::new();
    /// assert!(a.is_empty());
    /// a.insert(1, "a");
    /// assert!(!a.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        return self.len() == 0;
    }
}

// Implementation of versioning functions
impl<K: Ord, V> RollbackMap<K, V> {
    /// Returns checkpoint if created, that can be used to rollback to.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.get_last_checkpoint(), None);
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// let checkpoint = map.checkpoint();
    /// assert!(checkpoint.is_some());
    /// map.insert(1, "xa");
    /// map.remove(&2);
    /// map.rollback(checkpoint.unwrap());
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), Some(&"b"));
    /// ```
    pub fn checkpoint(&mut self) -> Option<u32> {
        if let Some(last) = self.versions.last() {
            let version = last.checkpoint;
            let values_count = last.values_count;
            self.versions
                .push(VersionState::new(version + 1, values_count));
            return Some(version);
        }
        return None;
    }

    /// Returns last created checkpoint if any.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.get_last_checkpoint(), None);
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// assert!(map.checkpoint().is_some());
    /// map.insert(1, "xa");
    /// map.remove(&2);
    /// assert!(map.rollback(map.get_last_checkpoint().unwrap()));
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), Some(&"b"));
    /// ```
    pub fn get_last_checkpoint(&self) -> Option<u32> {
        if self.versions.len() < 2 {
            return None;
        }

        let prev_index = self.versions.len() - 2;

        return Some(self.versions[prev_index].checkpoint);
    }

    /// Returns checkpoint before the last one saved.
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.get_last_checkpoint(), None);
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// let first_checkpoint = map.checkpoint();
    /// assert!(first_checkpoint.is_some());
    /// assert_eq!(map.get_prev_checkpoint(), None);
    /// map.insert(1, "xa");
    /// map.remove(&2);
    /// let second_checkpoint = map.checkpoint();
    /// assert!(second_checkpoint.is_some());
    /// map.insert(1, "xb");
    /// map.insert(2, "xc");
    /// assert_eq!(map.get(&1), Some(&"xb"));
    /// assert_eq!(map.get(&2), Some(&"xc"));
    /// assert_eq!(map.get_prev_checkpoint(), first_checkpoint);
    /// assert!(map.rollback(first_checkpoint.unwrap()));
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), Some(&"b"));
    /// assert_eq!(false, map.rollback(second_checkpoint.unwrap()));
    /// ```
    pub fn get_prev_checkpoint(&self) -> Option<u32> {
        if self.versions.len() < 3 {
            return None;
        }
        let prev_index = self.versions.len() - 3;
        return Some(self.versions[prev_index].checkpoint);
    }

    // Returns checkpoint count.
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map : RollbackMap<u32, &str> = RollbackMap::new();
    /// let last_checkpoint = map.get_last_checkpoint();
    /// assert_eq!(last_checkpoint, None);
    /// map.insert(1, "a");
    /// map.checkpoint();
    /// assert_eq!(map.get_checkpoints_count(), 1);
    /// map.insert(2, "b");
    /// map.checkpoint();
    /// assert_eq!(map.get_checkpoints_count(), 2);
    /// ```
    pub fn get_checkpoints_count(&self) -> usize {
        if self.versions.len() < 2 {
            return 0;
        }

        return self.versions.len() - 1;
    }

    /// Rollbacks to saved checkpoint.
    /// Rollback is only possible in backward direction.
    /// If the rollback is done successfully, true is returned, false otherwise.
    /// Successful rollback deletes all the changes that were done the provided checkpoint.
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.get_last_checkpoint(), None);
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// let first_checkpoint = map.checkpoint();
    /// assert!(first_checkpoint.is_some());
    /// assert_eq!(map.get_prev_checkpoint(), None);
    /// map.insert(1, "xa");
    /// map.remove(&2);
    /// let second_checkpoint = map.checkpoint();
    /// assert!(second_checkpoint.is_some());
    /// map.insert(1, "xb");
    /// map.insert(2, "xc");
    /// assert_eq!(map.get(&1), Some(&"xb"));
    /// assert_eq!(map.get(&2), Some(&"xc"));
    /// assert_eq!(map.get_prev_checkpoint(), first_checkpoint);
    /// assert!(map.rollback(first_checkpoint.unwrap()));
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), Some(&"b"));
    /// assert_eq!(false, map.rollback(second_checkpoint.unwrap()));
    /// ```
    pub fn rollback(&mut self, checkpoint: u32) -> bool {
        let mut found = false;
        let mut values_count = 0;
        for version in self.versions.iter().rev() {
            if version.checkpoint == checkpoint {
                found = true;
                values_count = version.values_count;
                break;
            }
        }

        if !found {
            return false;
        }

        let mut rollback = false;
        while self.versions.len() >= 2 {
            if let Some(last_checkpoint) = self.get_last_checkpoint() {
                if last_checkpoint == checkpoint {
                    if let Some(last) = self.versions.last_mut() {
                        last.reset(values_count);
                        rollback = true;
                        break;
                    }
                }
            }
            self.versions.pop();
        }

        return rollback;
    }

    /// Deletes all the checkpoints except the last one.
    /// Returns the last saved checkpoint if any.
    ///
    /// # Examples
    ///
    /// Basic usage:
    /// ```
    /// use crate::rollbackmap::RollbackMap;
    ///
    /// let mut map = RollbackMap::new();
    /// assert_eq!(map.get_last_checkpoint(), None);
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// let first_checkpoint = map.checkpoint();
    /// assert!(first_checkpoint.is_some());
    /// assert_eq!(map.get_prev_checkpoint(), None);
    /// map.insert(1, "xa");
    /// map.remove(&2);
    /// let second_checkpoint = map.checkpoint();
    /// assert!(second_checkpoint.is_some());
    /// map.insert(1, "xb");
    /// map.insert(2, "xc");
    /// assert_eq!(map.get(&1), Some(&"xb"));
    /// assert_eq!(map.get(&2), Some(&"xc"));
    /// assert_eq!(map.prune(), second_checkpoint);
    /// assert_eq!(map.get(&1), Some(&"xb"));
    /// assert_eq!(map.get(&2), Some(&"xc"));
    /// assert_eq!(false, map.rollback(first_checkpoint.unwrap()));
    /// assert_eq!(true, map.rollback(second_checkpoint.unwrap()));
    /// assert_eq!(map.get(&1), Some(&"xa"));
    /// assert_eq!(map.get(&2), None);
    /// ```
    pub fn prune(&mut self) -> Option<u32> {
        while self.versions.len() > 2 {
            self.versions.remove(0);
        }
        return self.get_last_checkpoint();
    }
}
