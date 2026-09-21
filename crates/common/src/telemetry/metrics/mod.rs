/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod otel;
pub mod prometheus;

// Clean-room AGPL reimplementation (see store.rs); compiled unconditionally.
pub mod store;

// Clean-room AGPL reimplementation of metric alerts; compiled unconditionally.
pub mod alerts;

#[cfg(any(feature = "dev_mode", feature = "test_mode"))]
pub mod test_data;
