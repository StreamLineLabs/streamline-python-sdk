//! Python bindings for Embedded Streamline.
//!
//! Provides a Python-native API for running Streamline in-process,
//! without Docker or external processes. Ideal for testing.
//!
//! **Status: API scaffold only.** The Streamline C FFI is not yet linked,
//! so every storage operation below (`create_topic`, `delete_topic`,
//! `produce`, `consume`, `list_topics`, `latest_offset`, `flush`) raises
//! `NotImplementedError` rather than silently doing nothing and reporting
//! success. Do not use this crate as a functional in-process broker.
//!
//! ## Planned usage (Python) — once the FFI is linked
//!
//! ```python
//! from streamline_embedded import EmbeddedStreamline
//!
//! # Start an in-memory instance
//! sl = EmbeddedStreamline.in_memory()
//!
//! # Create a topic
//! sl.create_topic("events", partitions=3)
//!
//! # Produce and consume
//! sl.produce("events", partition=0, key=b"k1", value=b"hello")
//! records = sl.consume("events", partition=0, offset=0, max_records=10)
//! for r in records:
//!     print(f"offset={r.offset} value={r.value}")
//!
//! sl.close()
//! ```

use pyo3::prelude::*;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};

/// Build the human-readable message for a not-yet-implemented storage
/// operation. Kept as a plain Rust function (no PyO3 types) so it can be
/// unit-tested with `cargo test` without needing a linked/initialized
/// CPython interpreter (the `extension-module` PyO3 feature intentionally
/// does not link libpython for a standalone `cargo test` binary).
fn ffi_not_implemented_message(operation: &str) -> String {
    format!(
        "EmbeddedStreamline.{operation}() is not implemented: this is an \
         API scaffold and the Streamline C FFI is not yet linked. No \
         storage operation has been performed; do not treat this instance \
         as a functional in-process broker."
    )
}

/// Build the error raised by every storage operation that requires the
/// (not-yet-linked) Streamline C FFI. These methods must never fabricate a
/// successful result — doing so would silently lie to callers about
/// whether their topic/produce/consume/etc. call actually happened.
fn ffi_not_implemented(operation: &str) -> PyErr {
    PyNotImplementedError::new_err(ffi_not_implemented_message(operation))
}


/// A record returned by consume operations.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct Record {
    #[pyo3(get)]
    offset: i64,
    #[pyo3(get)]
    timestamp: i64,
    #[pyo3(get)]
    key: Option<Vec<u8>>,
    #[pyo3(get)]
    value: Vec<u8>,
}

#[pymethods]
impl Record {
    fn __repr__(&self) -> String {
        format!(
            "Record(offset={}, key={:?}, value={} bytes)",
            self.offset,
            self.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
            self.value.len()
        )
    }
}
/// Embedded Streamline instance — runs in-process, no Docker needed.
///
/// Use `EmbeddedStreamline.in_memory()` for ephemeral testing or
/// `EmbeddedStreamline(data_dir="/path")` for persistent storage.
#[pyclass]
struct EmbeddedStreamline {
    // In production, this holds a *mut StreamlineHandle from the C FFI.
    // For the scaffold, we use a placeholder that documents the API shape.
    _data_dir: Option<String>,
    _closed: bool,
}

#[pymethods]
impl EmbeddedStreamline {
    /// Create an embedded instance with persistent storage.
    #[new]
    #[pyo3(signature = (data_dir=None, default_partitions=1))]
    fn new(data_dir: Option<String>, default_partitions: i32) -> PyResult<Self> {
        let _ = default_partitions; // Used when FFI is linked
        Ok(Self {
            _data_dir: data_dir,
            _closed: false,
        })
    }

    /// Create an in-memory instance (no persistence, fastest for tests).
    #[staticmethod]
    fn in_memory() -> PyResult<Self> {
        Ok(Self {
            _data_dir: None,
            _closed: false,
        })
    }

    /// Create a topic with the given number of partitions.
    ///
    /// Not implemented: the Streamline C FFI is not linked in this
    /// scaffold, so no topic is actually created. Raises
    /// `NotImplementedError` rather than returning fake success.
    fn create_topic(&self, name: &str, partitions: Option<i32>) -> PyResult<()> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (name, partitions);
        Err(ffi_not_implemented("create_topic"))
    }

    /// Delete a topic.
    ///
    /// Not implemented: see `create_topic`.
    fn delete_topic(&self, name: &str) -> PyResult<()> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = name;
        Err(ffi_not_implemented("delete_topic"))
    }

    /// Produce a record to a topic partition.
    ///
    /// Not implemented: no record is actually written and no offset is
    /// actually allocated. Raises `NotImplementedError` rather than
    /// returning a fabricated offset (which would look like a successful
    /// produce to callers).
    fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> PyResult<i64> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition, key, value);
        Err(ffi_not_implemented("produce"))
    }

    /// Consume records from a topic partition.
    ///
    /// Not implemented: returning an empty list here would look
    /// indistinguishable from "no records available yet", silently lying
    /// to callers. Raises `NotImplementedError` instead.
    fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: Option<i32>,
    ) -> PyResult<Vec<Record>> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition, offset, max_records);
        Err(ffi_not_implemented("consume"))
    }

    /// List all topics.
    ///
    /// Not implemented: see `create_topic`.
    fn list_topics(&self) -> PyResult<Vec<String>> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        Err(ffi_not_implemented("list_topics"))
    }

    /// Get the latest offset for a partition.
    ///
    /// Not implemented: see `produce`.
    fn latest_offset(&self, topic: &str, partition: i32) -> PyResult<i64> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition);
        Err(ffi_not_implemented("latest_offset"))
    }

    /// Flush all pending writes.
    ///
    /// Not implemented: there is nothing to flush because `produce` never
    /// actually writes anything in this scaffold.
    fn flush(&self) -> PyResult<()> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        Err(ffi_not_implemented("flush"))
    }

    /// Close the instance and free resources.
    fn close(&mut self) -> PyResult<()> {
        self._closed = true;
        Ok(())
    }

    /// Get the Streamline version.
    #[staticmethod]
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// Streamline Embedded Python Module
#[pymodule]
fn streamline_embedded(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EmbeddedStreamline>()?;
    m.add_class::<Record>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Placeholder storage operations must build a message (never
    /// fabricate success) that clearly names the operation and warns the
    /// caller not to treat the scaffold as functional. This only exercises
    /// the plain-Rust message builder — not `PyErr` — because formatting a
    /// `PyErr` requires a linked/initialized CPython interpreter, which
    /// the `extension-module` build intentionally does not provide to a
    /// standalone `cargo test` binary.
    #[test]
    fn ffi_not_implemented_message_names_operation_and_warns_no_op_performed() {
        for op in [
            "create_topic",
            "delete_topic",
            "produce",
            "consume",
            "list_topics",
            "latest_offset",
            "flush",
        ] {
            let message = ffi_not_implemented_message(op);
            assert!(
                message.contains(op),
                "message for {op:?} should name the operation: {message:?}"
            );
            assert!(
                message.contains("not implemented"),
                "message for {op:?} should say not implemented: {message:?}"
            );
            assert!(
                message.contains("No storage operation has been performed"),
                "message for {op:?} should disclaim fake success: {message:?}"
            );
        }
    }

    #[test]
    fn ffi_not_implemented_message_is_unique_per_operation() {
        let a = ffi_not_implemented_message("produce");
        let b = ffi_not_implemented_message("consume");
        assert_ne!(a, b);
    }
}
