package org.apache.flink.table.connector.source;

import org.apache.flink.api.connector.source.RuntimeFilter;
import org.apache.flink.table.data.RowData;

public class DynamicFilteringDataRuntimeFilter implements RuntimeFilter<RowData> {
    private final DynamicFilteringData data;

    public DynamicFilteringDataRuntimeFilter(DynamicFilteringData data) {
        this.data = data;
    }

    @Override
    public boolean match(RowData row) {
        return data.contains(row);
    }

    public DynamicFilteringData getDynamicFilterData() {
        return data;
    }
}
