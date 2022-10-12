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
import java.util.concurrent.TimeUnit;

public class TimingSnapshotTriggerStrategy implements SnapshotTriggerStrategy {

    private final long timingMs;

    private long lastSnapshotTime;

    private TimingSnapshotTriggerStrategy(long timingMs) {
        this.timingMs = timingMs;
    }

    public static TimingSnapshotTriggerStrategy of(long duration, TimeUnit unit) {
        return new TimingSnapshotTriggerStrategy(unit.toMillis(duration));
    }

    @Override
    public void loadStateWhenCommit(DLedgerEntry dLedgerEntry) {
        // we regard the first commit as the first snapshot time
        if (this.lastSnapshotTime == 0) {
            this.lastSnapshotTime = System.currentTimeMillis();
        }
    }

    @Override
    public void loadStateWhenSnapshotUpdate(SnapshotMeta snapshotMeta) {
        this.lastSnapshotTime = System.currentTimeMillis();
    }

    @Override
    public boolean triggerSnapshot(DLedgerEntry dLedgerEntry) {
        return System.currentTimeMillis() - lastSnapshotTime >= timingMs;
    }
}
