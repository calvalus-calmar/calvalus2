package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L3 production.
 *
 * @author Norman
 */
public class OrderMAProductionView extends OrderProductionView {
    public static final String ID = OrderMAProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private MAConfigForm maConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderMAProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                maConfigForm.setProcessorDescriptor(l2ConfigForm.getProcessorDescriptor());
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getProductSet());
        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                // ?
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
                // ?
            }
        });

        maConfigForm = new MAConfigForm(portalContext);
        maConfigForm.setProcessorDescriptor(l2ConfigForm.getProcessorDescriptor());

        outputParametersForm = new OutputParametersForm(false);

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(maConfigForm);
        panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

        this.widget = panel;
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public String getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Match-up Analysis";
    }

    @Override
    protected String getProductionType() {
        return "MA";
    }

    @Override
    public void onShowing() {
        // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
        productSetFilterForm.getRegionMap().getMapWidget().checkResizeAndCenter();
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetSelectionForm.validateForm();
            productSetFilterForm.validateForm();
            l2ConfigForm.validateForm();
            maConfigForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    // todo - Provide JUnit test for this method
    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(maConfigForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }
}