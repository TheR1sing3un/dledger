/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.snapshot.strategy.impl;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.strategy.SnapshotTriggerStrategy;

public class BytesNumSnapshotTriggerStrategy implements SnapshotTriggerStrategy {

    public static final long DEFAULT_BYTES_THRESHOLD = 512 * 1024 * 1024;

    private final long bytesThreshold;

    private long firstEntryPos = 0;

    private long nextEntryPos = 0;

    private BytesNumSnapshotTriggerStrategy(long bytesThreshold) {
        this.bytesThreshold = bytesThreshold;
    }

    public static BytesNumSnapshotTriggerStrategy of() {
        return new BytesNumSnapshotTriggerStrategy(DEFAULT_BYTES_THRESHOLD);
    }

    public static BytesNumSnapshotTriggerStrategy of(long bytesThreshold) {
        return new BytesNumSnapshotTriggerStrategy(bytesThreshold);
    }

    @Override
    public void loadStateWhenCommit(DLedgerEntry dLedgerEntry) {
        this.nextEntryPos = dLedgerEntry.getPos() + dLedgerEntry.getSize();
    }

    @Override
    public void loadStateWhenSnapshotUpdate(SnapshotMeta snapshotMeta) {
        this.firstEntryPos = snapshotMeta.getAfterSnapshotNextEntryPos();
        this.nextEntryPos = this.firstEntryPos;
    }

    @Override
    public boolean triggerSnapshot(DLedgerEntry dLedgerEntry) {
        return this.nextEntryPos - this.firstEntryPos > this.bytesThreshold;
    }
}
