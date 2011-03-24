/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A collection of one or more work items.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class Workflow extends AbstractWorkflowItem implements WorkflowStatusListener {
    protected final List<WorkflowItem> itemList;

    protected Workflow(WorkflowItem... items) {
        super();
        this.itemList = new ArrayList<WorkflowItem>();
        add(items);
    }

    public void add(WorkflowItem... items) {
        for (WorkflowItem item : items) {
            item.addWorkflowStatusListener(this);
        }
        itemList.addAll(Arrays.asList(items));
    }

    @Override
    public void kill() throws WorkflowException {
        for (WorkflowItem item : itemList) {
            if (!item.getStatus().isDone()) {
                item.kill();
            }
        }
    }

    @Override
    public void updateStatus() {
        for (WorkflowItem item : itemList) {
            item.updateStatus();
        }
    }

    /**
     * Aggregates statuses from its items. Called whenever status changes in items are detected.
     * There is usually no need to call this method directly.
     *
     * @param event The workflow status event.
     */
    @Override
    public void handleStatusChanged(WorkflowStatusEvent event) {
        ProcessStatus[] statuses = new ProcessStatus[itemList.size()];
        Date submitTime = null;
        Date startTime = null;
        Date stopTime = null;
        for (int i = 0; i < statuses.length; i++) {
            WorkflowItem workflowItem = itemList.get(i);
            statuses[i] = workflowItem.getStatus();

            Date itemSubmitTime = workflowItem.getSubmitTime();
            if (submitTime == null || itemSubmitTime != null && itemSubmitTime.before(submitTime)) {
                submitTime = itemSubmitTime;
            }
            Date itemStartTime = workflowItem.getStartTime();
            if (startTime == null || itemStartTime != null && itemStartTime.before(startTime)) {
                startTime = itemStartTime;
            }
            Date itemStopTime = workflowItem.getStopTime();
            if (stopTime == null || itemStopTime != null && itemStopTime.after(stopTime)) {
                stopTime = itemStopTime;
            }
        }
        setStatus(ProcessStatus.aggregate(statuses));
        setSubmitTime(submitTime);
        setStartTime(startTime);
        if (getStatus().getState().isDone()) {
            setStopTime(stopTime);
        }
    }

    @Override
    @Deprecated
    public Object[] getJobIds() {
        ArrayList<Object> list = new ArrayList<Object>();
        for (WorkflowItem item : itemList) {
            list.addAll(Arrays.asList(item.getJobIds()));
        }
        return list.toArray(new Object[list.size()]);
    }

    @Override
    public WorkflowItem[] getItems() {
        return itemList.toArray(new WorkflowItem[itemList.size()]);
    }

    /**
     * A sequential workflow. An item is submitted only after its predecessor has completed.
     */
    public static class Sequential extends Workflow {
        private int currentItemIndex;

        public Sequential(WorkflowItem... items) {
            super(items);
            currentItemIndex = -1;
        }

        /**
         * Submits the first item. Subsequent items are submitted after their predecessors have successfully completed.
         */
        @Override
        public void submit() throws WorkflowException {
            submitNext();
        }

        @Override
        public void handleStatusChanged(WorkflowStatusEvent event) {
            super.handleStatusChanged(event);
            if (currentItemIndex >= 0
                    && event.getSource() == itemList.get(currentItemIndex)
                    && event.getNewStatus().getState() == ProcessState.COMPLETED) {
                submitNext();
            }
        }

        private void submitNext() {
            if (currentItemIndex < itemList.size() - 1) {
                currentItemIndex++;
                try {
                    itemList.get(currentItemIndex).submit();
                } catch (WorkflowException e) {
                    itemList.get(currentItemIndex).setStatus(new ProcessStatus(ProcessState.ERROR, 0.0F, e.getMessage()));
                }
            }
        }
    }

    /**
     * A parallel workflow. All items are submitted independently of each other.
     */
    public static class Parallel extends Workflow {

        public Parallel(WorkflowItem... items) {
            super(items);
        }

        /**
         * Submits all contained items at once.
         */
        @Override
        public void submit() throws WorkflowException {
            for (WorkflowItem item : itemList) {
                item.submit();
            }
        }
    }
}
