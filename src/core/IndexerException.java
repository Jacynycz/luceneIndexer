/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

/**
 *
 * @author usuario
 */
public class IndexerException extends Exception {

	 public IndexerException(String message) {
			super(message);
	 }

	 @Override
	 public String getMessage() {
			return "Error en el índice: " + super.getMessage();
	 }

}
