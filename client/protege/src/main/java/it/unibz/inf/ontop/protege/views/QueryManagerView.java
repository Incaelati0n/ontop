package it.unibz.inf.ontop.protege.views;

/*
 * #%L
 * ontop-protege
 * %%
 * Copyright (C) 2009 - 2013 KRDB Research Centre. Free University of Bozen Bolzano.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import it.unibz.inf.ontop.protege.core.OBDAEditorKitSynchronizerPlugin;
import it.unibz.inf.ontop.protege.core.OBDAModelManager;
import it.unibz.inf.ontop.protege.panels.SavedQueriesPanel;
import it.unibz.inf.ontop.protege.panels.SavedQueriesPanelListener;

import java.awt.BorderLayout;

import org.protege.editor.owl.ui.view.AbstractOWLViewComponent;

public class QueryManagerView extends AbstractOWLViewComponent {

	private static final long serialVersionUID = 1L;
	
	private SavedQueriesPanel panel;

	@Override
	protected void disposeOWLView() {
		QueryManagerViewsList queryManagerViews = (QueryManagerViewsList) getOWLEditorKit().get(QueryManagerViewsList.class.getName());
		if (queryManagerViews != null)
			queryManagerViews.remove(this);
	}

	@Override
	protected void initialiseOWLView()  {
		OBDAModelManager obdaController = OBDAEditorKitSynchronizerPlugin.getOBDAModelManager(getOWLEditorKit());

		setLayout(new BorderLayout());
		panel = new SavedQueriesPanel(obdaController.getQueryController());

		add(panel, BorderLayout.CENTER);

		QueryManagerViewsList queryManagerViews = (QueryManagerViewsList) getOWLEditorKit().get(QueryManagerViewsList.class.getName());
		if (queryManagerViews == null) {
			queryManagerViews = new QueryManagerViewsList();
			getOWLEditorKit().put(QueryManagerViewsList.class.getName(), queryManagerViews);
		}

		QueryInterfaceViewsList queryInterfaceViews = (QueryInterfaceViewsList) getOWLEditorKit().get(QueryInterfaceViewsList.class.getName());
		if (queryInterfaceViews != null)
			for (QueryInterfaceView queryInterfaceView : queryInterfaceViews)
				this.addListener(queryInterfaceView);

		queryManagerViews.add(this);
	}

	public void addListener(SavedQueriesPanelListener listener) {
		panel.addQueryManagerListener(listener);
	}

	public void removeListener(SavedQueriesPanelListener listener) {
		panel.removeQueryManagerListener(listener);
	}
}
