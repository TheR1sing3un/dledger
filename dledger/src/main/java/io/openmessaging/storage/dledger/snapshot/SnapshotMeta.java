/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.snapshot;

public class SnapshotMeta {

    private long lastIncludedEntryIndex;
    private long lastIncludedEntryTerm;

    private long afterSnapshotNextEntryPos;

    public SnapshotMeta(long lastIncludedEntryIndex, long lastIncludedTerm, long afterSnapshotNextEntryPos) {
        this.lastIncludedEntryIndex = lastIncludedEntryIndex;
        this.lastIncludedEntryTerm = lastIncludedTerm;
        this.afterSnapshotNextEntryPos = afterSnapshotNextEntryPos;
    }

    public long getLastIncludedEntryIndex() {
        return lastIncludedEntryIndex;
    }

    public void setLastIncludedEntryIndex(int lastIncludedEntryIndex) {
        this.lastIncludedEntryIndex = lastIncludedEntryIndex;
    }

    public long getLastIncludedEntryTerm() {
        return lastIncludedEntryTerm;
    }

    public void setLastIncludedEntryTerm(int lastIncludedEntryTerm) {
        this.lastIncludedEntryTerm = lastIncludedEntryTerm;
    }

    public long getAfterSnapshotNextEntryPos() {
        return afterSnapshotNextEntryPos;
    }

    @Override
    public String toString() {
        return "SnapshotMeta{" +
            "lastIncludedEntryIndex=" + lastIncludedEntryIndex +
            ", lastIncludedEntryTerm=" + lastIncludedEntryTerm +
            ", afterSnapshotNextEntryPos=" + afterSnapshotNextEntryPos +
            '}';
    }
}
