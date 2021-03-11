package it.unibz.inf.ontop.exception;

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

import org.antlr.v4.runtime.RecognitionException;

public class TargetQueryParserException extends Exception {
	
	private static final long serialVersionUID = 8515860690059565681L;

	private final int line, column;

	public TargetQueryParserException(String message, Throwable cause) {
		super(message, cause);
		line = 0;
		column = 0;
	}

	public TargetQueryParserException(int line, int column, String message, RecognitionException e) {
		super(message, e);
		this.line = line;
		this.column = column;
	}

	public int getLine() { return line; }
	public int getColumn() { return column; }
}
