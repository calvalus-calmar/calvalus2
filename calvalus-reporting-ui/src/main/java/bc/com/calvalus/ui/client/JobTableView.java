package bc.com.calvalus.ui.client;

import bc.com.calvalus.ui.shared.UserInfo;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.builder.shared.TableRowBuilder;
import com.google.gwt.dom.client.Style;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.cellview.client.AbstractHeaderOrFooterBuilder;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.ColumnSortEvent;
import com.google.gwt.user.cellview.client.DataGrid;
import com.google.gwt.user.cellview.client.SimplePager;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DockPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.view.client.ListDataProvider;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author muhammad.bc.
 */

public class JobTableView<T> extends Composite {
    private static final String WIDTH_100_PERCENT = "100%";
    private static final boolean SHOW_FAST_FORWARD_BUTTON = false;
    private static final int FAST_FORWARD_ROWS = 0;
    private static final boolean SHOW_LAST_PAGE_BUTTON = true;
    public static final String DATE_COLUMN = "Date";
    public static final String USER_COLUMN = "User";
    public static final String QUEUE_COLUMN = "Queue";
    public static final String PRODUCTS_COLUMN = "Products";
    public static final String CPU_HOURS_COLUMN = "Cpu Hours";
    public static final String RAM_GB_HOURS_COLUMN = "RAM GB hours";
    public static final String GB_READ_COLUMN = "GB Read";
    public static final String GB_WRITE_COLUMN = "GB Write";
    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    private DataGrid<T> dataGrid;
    private ListDataProvider<T> dataProvider;
    private List<T> dataList;
    private DockPanel dock = new DockPanel();
    private final ColumnSortEvent.ListHandler<T> sortHandler;

    JobTableView() {
        initWidget(dock);
        dataGrid = new DataGrid<T>();
        dataGrid.setWidth(WIDTH_100_PERCENT);
        dataGrid.setHeight("700px");

        SimplePager.Resources pagerResources = GWT.create(SimplePager.Resources.class);
        SimplePager pager = new SimplePager(SimplePager.TextLocation.CENTER, pagerResources, SHOW_FAST_FORWARD_BUTTON, FAST_FORWARD_ROWS, SHOW_LAST_PAGE_BUTTON);
        pager.setDisplay(dataGrid);
        dataProvider = new ListDataProvider<T>();
        dataProvider.setList(new ArrayList<T>());
        dataGrid.setEmptyTableWidget(new HTML("No Data to Display"));
        sortHandler = new ColumnSortEvent.ListHandler<T>(dataProvider.getList());

        initTableColumns(dataGrid, sortHandler);

        dataGrid.addColumnSortHandler(sortHandler);
        dataProvider.addDataDisplay(dataGrid);
        pager.setVisible(true);
        dataGrid.setVisible(true);
//        dataGrid.setFooterBuilder(new SumColumnValueFooterBuilder());

        dock.add(dataGrid, DockPanel.CENTER);
        dock.add(pager, DockPanel.SOUTH);
        dock.setWidth(WIDTH_100_PERCENT);
        dock.setCellWidth(dataGrid, WIDTH_100_PERCENT);
        dock.setCellWidth(pager, WIDTH_100_PERCENT);
        dock.setCellHorizontalAlignment(pager, HasHorizontalAlignment.ALIGN_CENTER);
    }

    void setDataList(List<T> dataList) {
        List<T> list = dataProvider.getList();
        list.clear();
        if (dataList == null) {
            dataGrid.setEmptyTableWidget(new HTML("No Data to Display"));
        } else {
            list.addAll(dataList);
        }
        dataProvider.refresh();
        dataGrid.redraw();

    }

    void initDateTable() {
        initNewColumn();
        Column dateTime = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInDate();
            }
        };
        dateTime.setSortable(true);
        sortHandler.setComparator(dateTime, (o1, o2) -> {
            String first = ((UserInfo) o1).getJobsInDate();
            String second = ((UserInfo) o2).getJobsInDate();
            Date firstDate = DateTimeFormat.getFormat(YYYY_MM_DD).parse(first);
            Date secondDate = DateTimeFormat.getFormat(YYYY_MM_DD).parse(second);
            return firstDate.compareTo(secondDate);
        });
        dataGrid.insertColumn(0, dateTime, DATE_COLUMN);
        dataGrid.setColumnWidth(dateTime, 10, Style.Unit.EM);
    }

    void initUserTable() {
        initNewColumn();
        Column user = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getUser();
            }
        };
        user.setSortable(true);
        sortHandler.setComparator(user, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return o11.compareTo(o12);
        });
        dataGrid.insertColumn(0, user, USER_COLUMN);
        dataGrid.setColumnWidth(user, 10, Style.Unit.EM);
    }

    void initQueueTable() {
        initNewColumn();
        Column queue = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInQueue();
            }
        };
        queue.setSortable(true);
        sortHandler.setComparator(queue, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return o11.getJobsInQueue().compareTo(o12.getJobsInQueue());
        });
        dataGrid.insertColumn(0, queue, QUEUE_COLUMN);
        dataGrid.setColumnWidth(queue, 10, Style.Unit.EM);
    }

    private void initNewColumn() {
        dataGrid.removeColumn(0);
        List<T> list = dataProvider.getList();
        list.clear();
        dataProvider.refresh();
        dataGrid.redraw();
    }


    private void initTableColumns(DataGrid<T> dataGrid, ColumnSortEvent.ListHandler<T> sortHandler) {
        Column dateTime = new Column<UserInfo, String>(new TextCell()) {
            @Override
            public String getValue(UserInfo object) {
                return object.getJobsInDate();
            }
        };
        dateTime.setSortable(true);
        sortHandler.setComparator(dateTime, (o1, o2) -> {
            DateTimeFormat format = DateTimeFormat.getFormat(YYYY_MM_DD);
            Date firstDate = format.parse(((UserInfo) o1).getJobsInDate());
            Date secondDate = format.parse(((UserInfo) o2).getJobsInDate());
            return firstDate.compareTo(secondDate);
        });
        dataGrid.insertColumn(0, dateTime, DATE_COLUMN);
        dataGrid.setColumnWidth(dateTime, 10, Style.Unit.EM);

        // ### jobColumn
        Column<T, String> jobColumn = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getJobsProcessed();
            }
        };

        jobColumn.setSortable(true);
        sortHandler.setComparator(jobColumn, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;

            Integer integerO = new Integer(o11.getJobsProcessed());
            Integer integerM = new Integer(o12.getJobsProcessed());
            return integerO.compareTo(integerM);
        });
        dataGrid.addColumn(jobColumn, "Jobs");
        dataGrid.setColumnWidth(jobColumn, 20, Style.Unit.PCT);

        // #### products
        Column<T, String> products = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMapReduce();
            }
        };
        products.setSortable(true);
        sortHandler.setComparator(products, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return compareValues(o11.getTotalMapReduce(), o12.getTotalMapReduce());
        });
        dataGrid.addColumn(products, PRODUCTS_COLUMN);
        dataGrid.setColumnWidth(products, 20, Style.Unit.PCT);


        Column<T, String> cpuHours = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalCpuTimeSpent();
            }
        };
        cpuHours.setSortable(true);
        sortHandler.setComparator(cpuHours, (o1, o2) -> {
            DateTimeFormat format = DateTimeFormat.getFormat("hh:mm:ss");
            Date firstTime = format.parse(((UserInfo) o1).getTotalCpuTimeSpent());
            Date secondTime = format.parse(((UserInfo) o2).getTotalCpuTimeSpent());
            return firstTime.compareTo(secondTime);
        });
        dataGrid.addColumn(cpuHours, CPU_HOURS_COLUMN);
        dataGrid.setColumnWidth(cpuHours, 20, Style.Unit.PCT);


        Column<T, String> totalMemoryUsedMbs = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalMemoryUsedMbs();
            }
        };
        totalMemoryUsedMbs.setSortable(true);
        sortHandler.setComparator(totalMemoryUsedMbs, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return compareValues(o11.getTotalMemoryUsedMbs(), o12.getTotalMemoryUsedMbs());
        });
        dataGrid.addColumn(totalMemoryUsedMbs, RAM_GB_HOURS_COLUMN);
        dataGrid.setColumnWidth(totalMemoryUsedMbs, 20, Style.Unit.PCT);


        Column<T, String> totalFileReadingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileReadingMb();
            }
        };
        totalFileReadingMb.setSortable(true);
        sortHandler.setComparator(totalFileReadingMb, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return compareValues(o11.getTotalFileReadingMb(), o12.getTotalFileReadingMb());
        });
        dataGrid.addColumn(totalFileReadingMb, GB_READ_COLUMN);
        dataGrid.setColumnWidth(totalFileReadingMb, 20, Style.Unit.PCT);


        Column<T, String> totalFileWritingMb = new Column<T, String>(new TextCell()) {
            @Override
            public String getValue(T object) {
                return ((UserInfo) object).getTotalFileWritingMb();
            }
        };
        totalFileWritingMb.setSortable(true);
        sortHandler.setComparator(totalFileWritingMb, (o1, o2) -> {
            UserInfo o11 = (UserInfo) o1;
            UserInfo o12 = (UserInfo) o2;
            return compareValues(o11.getTotalFileWritingMb(), o12.getTotalFileWritingMb());
        });
        dataGrid.addColumn(totalFileWritingMb, GB_WRITE_COLUMN);
        dataGrid.setColumnWidth(totalFileWritingMb, 20, Style.Unit.PCT);
    }

    private int compareValues(String o11, String o12) {
        Float first = Float.valueOf(o11.replace(",", ""));
        Float second = Float.valueOf(o12.replace(",", ""));
        return first.compareTo(second);
    }


    private class SumColumnValueFooterBuilder extends AbstractHeaderOrFooterBuilder<T> {
        SumColumnValueFooterBuilder() {
            super(dataGrid, true);
        }

        @Override
        protected boolean buildHeaderOrFooterImpl() {
            List<UserInfo> visibleData = (List<UserInfo>) dataGrid.getVisibleItems();
            if (visibleData.size() > 0) {
                int totalTBWrite = 0;
                int totalTBRead = 0;
                int totalProduct = 0;

                for (UserInfo visibleDatum : visibleData) {
                    totalTBRead += Long.parseLong(visibleDatum.getTotalFileReadingMb());
                    totalProduct += Long.parseLong(visibleDatum.getJobsProcessed());
                    totalTBWrite += Long.parseLong(visibleDatum.getTotalFileWritingMb());
                }
            }
            TableRowBuilder tableRowBuilder = startRow();
            TableRowBuilder tableRow = tableRowBuilder.align(HasHorizontalAlignment.ALIGN_CENTER.getTextAlignString());
            return false;
        }
    }
}


