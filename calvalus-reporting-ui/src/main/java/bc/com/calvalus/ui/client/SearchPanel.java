package bc.com.calvalus.ui.client;import com.google.gwt.event.dom.client.ClickEvent;import com.google.gwt.event.dom.client.ClickHandler;import com.google.gwt.i18n.client.DateTimeFormat;import com.google.gwt.user.client.ui.Button;import com.google.gwt.user.client.ui.Composite;import com.google.gwt.user.client.ui.DecoratorPanel;import com.google.gwt.user.client.ui.FlexTable;import com.google.gwt.user.client.ui.HasHorizontalAlignment;import com.google.gwt.user.client.ui.HorizontalPanel;import com.google.gwt.user.client.ui.RadioButton;import com.google.gwt.user.client.ui.VerticalPanel;import com.google.gwt.user.client.ui.Widget;import com.google.gwt.user.datepicker.client.DateBox;import com.google.gwt.user.datepicker.client.DatePicker;import java.util.Date;/** * @author muhammad.bc. */public class SearchPanel extends Composite {    private JobResourcesServiceAsync resourcesServiceAsync;    private final DateBox startDate;    private final DateBox endDate;    private ColumnType columnType;    public SearchPanel() {        FlexTable flexTable = new FlexTable();        flexTable.setCellPadding(6);        FlexTable.FlexCellFormatter flexCellFormatter = flexTable.getFlexCellFormatter();        flexTable.setBorderWidth(0);        flexTable.setHTML(0, 0, "Report Selection");        flexCellFormatter.setColSpan(0, 0, 3);        flexCellFormatter.setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);        flexTable.setWidget(1, 0, addColumnSelectionOption());        flexCellFormatter.setColSpan(1, 0, 3);        startDate = getDateBox();        endDate = getDateBox();        flexTable.setHTML(2, 0, "Start Date:");        flexTable.setWidget(3, 0, startDate);        flexTable.setHTML(4, 0, "End Date:");        flexTable.setWidget(5, 0, endDate);        Widget flexTableWidget = searchButtons();        flexTable.setWidget(6, 0, flexTableWidget);        DecoratorPanel decoratorPanel = new DecoratorPanel();        decoratorPanel.setStyleName("searchPanel");        decoratorPanel.setWidget(flexTable);        initWidget(decoratorPanel);        columnType = ColumnType.DATE;    }    private Widget addColumnSelectionOption() {        HorizontalPanel horizontalPanel = new HorizontalPanel();        RadioButton user = new RadioButton("select", "User");        user.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                columnType = ColumnType.USER;                ReportUI.setColumnType(columnType);            }        });        RadioButton queue = new RadioButton("select", "Queue");        queue.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                columnType = ColumnType.QUEUE;                ReportUI.setColumnType(columnType);            }        });        RadioButton date = new RadioButton("select", "Date");        date.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                columnType = ColumnType.DATE;                ReportUI.setColumnType(columnType);            }        });        date.setValue(true);        horizontalPanel.add(user);        horizontalPanel.add(queue);        horizontalPanel.add(date);        return horizontalPanel;    }    public void updateDatePicker(String start, String end) {        startDate.getTextBox().setText(start);        endDate.getTextBox().setText(end);    }    private Widget searchButtons() {        Button searchButton = new Button("Search Record");        searchButton.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent event) {                ReportUI.searchRecordBetween(startDate.getTextBox().getValue(), endDate.getTextBox().getValue(), columnType);            }        });        Button todayBut = new Button("Today");        todayBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForToday(columnType);            }        });        Button yesterdayBut = new Button("Yesterday");        yesterdayBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForYesterday(columnType);            }        });        Button thisWeekBut = new Button("This Week");        thisWeekBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForThisWeek(columnType);            }        });        Button lastWeekBut = new Button("Last Week");        lastWeekBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForLastWeek(columnType);            }        });        Button thisMonthBut = new Button("  This Month  ");        thisMonthBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForThisMonth(columnType);            }        });        Button lastMonthBut = new Button("Last Month");        lastMonthBut.addClickHandler(new ClickHandler() {            @Override            public void onClick(ClickEvent clickEvent) {                ReportUI.searchRecordForLastMonth(columnType);            }        });        VerticalPanel verticalPanel = new VerticalPanel();        verticalPanel.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_CENTER);        FlexTable flexTable = new FlexTable();        flexTable.setCellPadding(4);        FlexTable.FlexCellFormatter flexCellFormatter = flexTable.getFlexCellFormatter();        flexTable.setBorderWidth(0);//        flexTable.setWidget(0, 0, searchButton);        flexTable.setWidget(1, 0, todayBut);        flexTable.setWidget(1, 1, yesterdayBut);        flexTable.setWidget(2, 0, thisWeekBut);        flexTable.setWidget(2, 1, lastWeekBut);        flexTable.setWidget(3, 0, thisMonthBut);        flexTable.setWidget(3, 1, lastMonthBut);        verticalPanel.add(searchButton);        verticalPanel.add(flexTable);        return verticalPanel;    }    private DateBox getDateBox() {        DateBox dateBox = new DateBox(new DatePicker(), null, new DateBox.DefaultFormat(DateTimeFormat.getFormat("yyyy-MM-dd")));        dateBox.setValue(new Date());        return dateBox;    }}