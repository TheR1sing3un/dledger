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

public class EntryNumSnapshotTriggerStrategy implements SnapshotTriggerStrategy {

    public static final int DEFAULT_SNAPSHOT_THRESHOLD = 1000;

    private final int snapshotThreshold;

    private long lastSnapshotIndex = -1;

    private long lastSnapshotTerm;

    private EntryNumSnapshotTriggerStrategy(int snapshotThreshold) {
        this.snapshotThreshold = snapshotThreshold;
    }

    public static EntryNumSnapshotTriggerStrategy of(int snapshotThreshold) {
        return new EntryNumSnapshotTriggerStrategy(snapshotThreshold);
    }

    public static EntryNumSnapshotTriggerStrategy of() {
        return new EntryNumSnapshotTriggerStrategy(DEFAULT_SNAPSHOT_THRESHOLD);
    }

    @Override
    public void loadStateWhenCommit(DLedgerEntry dLedgerEntry) {

    }

    @Override
    public void loadStateWhenSnapshotUpdate(SnapshotMeta snapshotMeta) {
        this.lastSnapshotIndex = snapshotMeta.getLastIncludedEntryIndex();
        this.lastSnapshotTerm = snapshotMeta.getLastIncludedEntryTerm();
    }

    @Override
    public boolean triggerSnapshot(DLedgerEntry dLedgerEntry) {
        return dLedgerEntry.getIndex() - this.lastSnapshotIndex >= this.snapshotThreshold;
    }
}
