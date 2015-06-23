/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.Serializable;

/**
 * Shard routing represents the state of a shard instance allocated in the cluster.
 */
public class ShardRouting implements Streamable, Serializable, ToXContent {

    private String index;
    private int shardId;
    protected String currentNodeId;
    protected String relocatingNodeId;
    protected boolean primary;
    protected ShardRoutingState state;
    protected long version;
    protected RestoreSource restoreSource;
    protected UnassignedInfo unassignedInfo;

    private transient ShardId shardIdentifier;
    private final transient ImmutableList<ShardRouting> asList;

    ShardRouting() {
        this.asList = ImmutableList.of(this);
    }

    public ShardRouting(ShardRouting copy) {
        this(copy, copy.getVersion());
    }

    public ShardRouting(ShardRouting copy, long version) {
        this(copy.getIndex(), copy.getId(), copy.getCurrentNodeId(), copy.getRelocatingNodeId(), copy.getRestoreSource(), copy.isPrimary(), copy.getState(), version, copy.getUnassignedInfo());
    }

    public ShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state, long version) {
        this(index, shardId, currentNodeId, null, primary, state, version);
    }

    public ShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, boolean primary, ShardRoutingState state, long version) {
        this(index, shardId, currentNodeId, relocatingNodeId, null, primary, state, version);
    }

    public ShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
        this(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version, null);
    }

    public ShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version,
                                 UnassignedInfo unassignedInfo) {
        this.index = index;
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.state = state;
        this.asList = ImmutableList.of(this);
        this.version = version;
        this.restoreSource = restoreSource;
        this.unassignedInfo = unassignedInfo;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
    }

    public String getIndex() {
        return this.index;
    }

    public int getId() {
        return this.shardId;
    }

    public long getVersion() {
        return this.version;
    }

    public boolean isUnassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    public boolean isInitializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    public boolean isActive() {
        return isStarted() || isRelocating();
    }

    public boolean isStarted() {
        return state == ShardRoutingState.STARTED;
    }

    public boolean isRelocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    public boolean isAssignedToNode() {
        return currentNodeId != null;
    }

    public String getCurrentNodeId() {
        return this.currentNodeId;
    }

    public String getRelocatingNodeId() {
        return this.relocatingNodeId;
    }

    public ShardRouting targetRoutingIfRelocating() {
        if (!isRelocating()) {
            return null;
        }
        return new ShardRouting(index, shardId, relocatingNodeId, currentNodeId, primary, ShardRoutingState.INITIALIZING, version);
    }

    public RestoreSource getRestoreSource() {
        return restoreSource;
    }

    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return unassignedInfo;
    }

    public boolean isPrimary() {
        return this.primary;
    }

    public ShardRoutingState getState() {
        return this.state;
    }

    public ShardId getShardId() {
        if (shardIdentifier != null) {
            return shardIdentifier;
        }
        shardIdentifier = new ShardId(index, shardId);
        return shardIdentifier;
    }

    public ShardIterator getShardsIt() {
        return new PlainShardIterator(getShardId(), asList);
    }

    public static ShardRouting readShardRoutingEntry(StreamInput in) throws IOException {
        ShardRouting entry = new ShardRouting();
        entry.readFrom(in);
        return entry;
    }

    public static ShardRouting readShardRoutingEntry(StreamInput in, String index, int shardId) throws IOException {
        ShardRouting entry = new ShardRouting();
        entry.readFrom(in, index, shardId);
        return entry;
    }

    public void readFrom(StreamInput in, String index, int shardId) throws IOException {
        this.index = index;
        this.shardId = shardId;
        readFromThin(in);
    }

    public void readFromThin(StreamInput in) throws IOException {
        version = in.readLong();
        if (in.readBoolean()) {
            currentNodeId = in.readString();
        }

        if (in.readBoolean()) {
            relocatingNodeId = in.readString();
        }

        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());

        restoreSource = RestoreSource.readOptionalRestoreSource(in);
        if (in.readBoolean()) {
            unassignedInfo = new UnassignedInfo(in);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        readFrom(in, in.readString(), in.readVInt());
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeLong(version);
        if (currentNodeId != null) {
            out.writeBoolean(true);
            out.writeString(currentNodeId);
        } else {
            out.writeBoolean(false);
        }

        if (relocatingNodeId != null) {
            out.writeBoolean(true);
            out.writeString(relocatingNodeId);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(primary);
        out.writeByte(state.value());

        if (restoreSource != null) {
            out.writeBoolean(true);
            restoreSource.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (unassignedInfo != null) {
            out.writeBoolean(true);
            unassignedInfo.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        writeToThin(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // we check on instanceof so we also handle the MutableShardRouting case as well
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }

        ShardRouting that = (ShardRouting) o;

        if (primary != that.primary) {
            return false;
        }
        if (shardId != that.shardId) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(that.currentNodeId) : that.currentNodeId != null) {
            return false;
        }
        if (index != null ? !index.equals(that.index) : that.index != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(that.relocatingNodeId) : that.relocatingNodeId != null) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (restoreSource != null ? !restoreSource.equals(that.restoreSource) : that.restoreSource != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        result = 31 * result + (currentNodeId != null ? currentNodeId.hashCode() : 0);
        result = 31 * result + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (restoreSource != null ? restoreSource.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(index).append(']').append('[').append(shardId).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (this.restoreSource != null) {
            sb.append(", restoring[").append(restoreSource).append("]");
        }
        sb.append(", s[").append(state).append("]");
        if (this.unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo.toString());
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("state", state)
                .field("primary", primary)
                .field("node", currentNodeId)
                .field("relocating_node", relocatingNodeId)
                .field("shard", shardId)
                .field("index", index);
        if (restoreSource != null) {
            builder.field("restore_source");
            restoreSource.toXContent(builder, params);
        }
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        return builder.endObject();
    }

    public static class Mutable extends ShardRouting {

        public Mutable(ShardRouting copy) {
            super(copy);
        }

        public Mutable(ShardRouting copy, long version) {
            super(copy, version);
        }

        public Mutable(String index, int shardId, String currentNodeId,
                                   String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
            super(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version);
            assert state != ShardRoutingState.UNASSIGNED : "new mutable routing should not be created with UNASSIGNED state, should moveToUnassigned";
        }

        /**
         * Moves the shard to unassigned state.
         */
        void moveToUnassigned(UnassignedInfo unassignedInfo) {
            this.version++;
            assert this.state != ShardRoutingState.UNASSIGNED;
            this.state = ShardRoutingState.UNASSIGNED;
            this.currentNodeId = null;
            this.relocatingNodeId = null;
            this.unassignedInfo = unassignedInfo;
        }

        /**
         * Assign this shard to a node.
         *
         * @param nodeId id of the node to assign this shard to
         */
        void assignToNode(String nodeId) {
            version++;

            if (currentNodeId == null) {
                assert state == ShardRoutingState.UNASSIGNED;

                state = ShardRoutingState.INITIALIZING;
                currentNodeId = nodeId;
                relocatingNodeId = null;
            } else if (state == ShardRoutingState.STARTED) {
                state = ShardRoutingState.RELOCATING;
                relocatingNodeId = nodeId;
            } else if (state == ShardRoutingState.RELOCATING) {
                assert nodeId.equals(relocatingNodeId);
            }
        }

        /**
         * Relocate the shard to another node.
         *
         * @param relocatingNodeId id of the node to relocate the shard
         */
        void relocate(String relocatingNodeId) {
            version++;
            assert state == ShardRoutingState.STARTED;
            state = ShardRoutingState.RELOCATING;
            this.relocatingNodeId = relocatingNodeId;
        }

        /**
         * Cancel relocation of a shard. The shards state must be set
         * to <code>RELOCATING</code>.
         */
        void cancelRelocation() {
            version++;
            assert state == ShardRoutingState.RELOCATING;
            assert isAssignedToNode();
            assert relocatingNodeId != null;

            state = ShardRoutingState.STARTED;
            relocatingNodeId = null;
        }

        /**
         * Moves the shard from started to initializing and bumps the version
         */
        void reinitializeShard() {
            assert state == ShardRoutingState.STARTED;
            version++;
            state = ShardRoutingState.INITIALIZING;
        }

        /**
         * Set the shards state to <code>STARTED</code>. The shards state must be
         * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
         * canceled.
         */
        void moveToStarted() {
            version++;
            assert state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.RELOCATING;
            relocatingNodeId = null;
            restoreSource = null;
            state = ShardRoutingState.STARTED;
            unassignedInfo = null; // we keep the unassigned data until the shard is started
        }

        /**
         * Make the shard primary unless it's not Primary
         * //TODO: doc exception
         */
        void moveToPrimary() {
            version++;
            if (primary) {
                throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
            }
            primary = true;
        }

        /**
         * Set the primary shard to non-primary
         */
        void moveFromPrimary() {
            version++;
            if (!primary) {
                throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
            }
            primary = false;
        }

        private long hashVersion = version-1;
        private int hashCode = 0;

        @Override
        public int hashCode() {
            hashCode = (hashVersion != version ? super.hashCode() : hashCode);
            hashVersion = version;
            return hashCode;
        }

    }
}
