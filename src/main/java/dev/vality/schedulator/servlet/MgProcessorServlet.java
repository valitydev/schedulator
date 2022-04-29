package dev.vality.schedulator.servlet;

import dev.vality.machinegun.stateproc.ProcessorSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/v1/processor")
@RequiredArgsConstructor
public class MgProcessorServlet extends GenericServlet {

    private final ProcessorSrv.Iface mgProcessorHandler;
    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(ProcessorSrv.Iface.class, mgProcessorHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
