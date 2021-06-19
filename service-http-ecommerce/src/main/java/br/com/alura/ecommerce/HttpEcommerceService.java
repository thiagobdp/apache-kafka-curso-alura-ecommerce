package br.com.alura.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Serve somente como um servidor HTTP para enviar requisições para nossos
 * serviços
 *
 */
public class HttpEcommerceService {

	public static void main(String[] args) throws Exception {
		var server = new Server(8080);

		var context = new ServletContextHandler();
		context.setContextPath("/");
		// no path "/new" ele executa a classe NewOrderServlet
		context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");
		// no path "/admin/generate-reports" ele executa a classe
		// GenerateAllReportsServlet
		context.addServlet(new ServletHolder(new GenerateAllReportsServlet()), "/admin/generate-reports");

		server.setHandler(context);

		server.start();
		server.join();
	}
}
