package it.unibz.inf.ontop.protege.panels;

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

import it.unibz.inf.ontop.model.term.functionsymbol.Predicate;
import it.unibz.inf.ontop.owlapi.validation.QuestOWLEmptyEntitiesChecker;
import it.unibz.inf.ontop.protege.utils.OBDAProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.util.Iterator;

public class EmptiesCheckPanel extends javax.swing.JPanel   implements OBDAProgressListener {

	private static final long serialVersionUID = 2317777246039649415L;

	Logger log = LoggerFactory.getLogger(EmptiesCheckPanel.class);

	private boolean bCancel = false;
	private boolean errorShown = false;

	public EmptiesCheckPanel() {
		initComponents();

	}

	public void initContent(QuestOWLEmptyEntitiesChecker check) {

		try {
		/* Create table list for empty concepts */

			Iterator<Predicate> emptyC = check.iEmptyConcepts();


			final int rowConcepts = check.getEConceptsSize();
			final int col = 1;
			final String[] columnConcept = {"Concepts"};

			Object[][] rowDataConcept = new Object[rowConcepts][col];

			JTable tblConceptCount = createTable(rowDataConcept, columnConcept);

			jScrollConcepts.setViewportView(tblConceptCount);

			DefaultTableModel modelConcept = (DefaultTableModel) tblConceptCount.getModel();

			while (emptyC.hasNext() && !bCancel) {
				modelConcept.addRow(new Object[]{emptyC.next().getName()});
			}

		/* Create table list for empty roles */
			Iterator<Predicate> emptyR = check.iEmptyRoles();

			final int rowRoles = check.getERolesSize();

			final String[] columnRole = {"Roles"};

			Object[][] rowDataRole = new Object[rowRoles][col];

			JTable tblRoleCount = createTable(rowDataRole, columnRole);

			jScrollRoles.setViewportView(tblRoleCount);

			DefaultTableModel modelRole = (DefaultTableModel) tblRoleCount.getModel();

			while (emptyR.hasNext() && !bCancel) {
				modelRole.addRow(new Object[]{emptyR.next().getName()});
			}


		/* Fill the label summary value */
			String message;
			try {
				message = check.toString();
			} catch (Exception e) {
				message = String.format("%s. Please try again!", e.getMessage());
			}
			lblSummaryValue.setText(message);

		}

		catch (Exception e) {
			errorShown = true;
			log.error("If OutOfMemoryError try increasing java heap size. ");
			e.printStackTrace();
			JOptionPane.showMessageDialog(this, "An error occurred. For more info, see the logs.");
		}

	}

	private JTable createTable(Object[][] rowData, String[] columnNames) {

		DefaultTableModel model = new DefaultTableModel(rowData, columnNames);

		@SuppressWarnings("serial")
		JTable table = new JTable(model) {
			// Create a model in which the cells can't be edited
			@Override
			public boolean isCellEditable(int row, int column) {
				// all cells false
				return false;
			}
		};
		return table;
	}

	/**
	 * This method is called from within the constructor to initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is always
	 * regenerated by the Form Editor.
	 */
	// <editor-fold defaultstate="collapsed"
	// <editor-fold defaultstate="collapsed"
	// <editor-fold defaultstate="collapsed"
	// desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		pnlSummary = new javax.swing.JPanel();
		lblSummary = new javax.swing.JLabel();
		lblSummaryValue = new javax.swing.JLabel();
		pnlEmptiesSummary = new javax.swing.JPanel();
		jScrollConcepts = new javax.swing.JScrollPane();
		jScrollRoles = new javax.swing.JScrollPane();

		setFont(new java.awt.Font("Arial", 0, 18)); // NOI18N
		setMinimumSize(new java.awt.Dimension(520, 400));
		setPreferredSize(new java.awt.Dimension(520, 400));
		setLayout(new java.awt.BorderLayout());

		pnlSummary.setMinimumSize(new java.awt.Dimension(156, 23));
		pnlSummary.setPreferredSize(new java.awt.Dimension(156, 23));
		pnlSummary.setLayout(new java.awt.FlowLayout(java.awt.FlowLayout.LEFT));

		lblSummary.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
		lblSummary.setText("Empty concepts and roles");
		pnlSummary.add(lblSummary);

		lblSummaryValue.setFont(new java.awt.Font("Tahoma", 1, 11)); // NOI18N
		pnlSummary.add(lblSummaryValue);

		add(pnlSummary, java.awt.BorderLayout.NORTH);

		pnlEmptiesSummary.setLayout(new javax.swing.BoxLayout(pnlEmptiesSummary, javax.swing.BoxLayout.PAGE_AXIS));
		pnlEmptiesSummary.add(jScrollConcepts);

		jScrollRoles.setCursor(new java.awt.Cursor(java.awt.Cursor.DEFAULT_CURSOR));
		pnlEmptiesSummary.add(jScrollRoles);

		add(pnlEmptiesSummary, java.awt.BorderLayout.CENTER);
	}// </editor-fold>//GEN-END:initComponents

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JScrollPane jScrollConcepts;
	private javax.swing.JScrollPane jScrollRoles;
	private javax.swing.JLabel lblSummary;
	private javax.swing.JLabel lblSummaryValue;
	private javax.swing.JPanel pnlEmptiesSummary;
	private javax.swing.JPanel pnlSummary;

	@Override
	public void actionCanceled() {
			bCancel = true;

	}

	@Override
	public boolean isCancelled() {
		return this.bCancel;
	}

	@Override
	public boolean isErrorShown() {
		return this.errorShown;
	}
	// End of variables declaration//GEN-END:variables
}
