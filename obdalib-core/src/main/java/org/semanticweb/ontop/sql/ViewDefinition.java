package org.semanticweb.ontop.sql;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
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

import org.semanticweb.ontop.sql.api.Attribute;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ViewDefinition extends DataDefinition {


	private static final long serialVersionUID = 3312336193514797486L;

	private final String statement;
	
	public ViewDefinition(String name, String statement) {
		this(name, statement, Collections.<Attribute>emptyList());
	}

    protected ViewDefinition(String name, String statement, List<Attribute> attributes) {
        super(name, attributes);
        this.statement = statement;
    }

	public String getStatement() {
		return statement;
	}

	@Override
	public String toString() {
		StringBuilder bf = new StringBuilder();
		bf.append(getName());
		bf.append("[");
		boolean comma = false;
		for (Attribute att : getAttributes()) {
			if (comma) 
				bf.append(",");
			
			bf.append(att);
			comma = true;
		}
		bf.append("]");

		bf.append(String.format("   (%s)", statement));
		return bf.toString();
	}

    @Override
    public DataDefinition cloneDefinition() {
        return new ViewDefinition(getName(), statement, getAttributes());
    }
}
