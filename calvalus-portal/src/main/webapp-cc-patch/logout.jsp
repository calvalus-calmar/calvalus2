<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<html>
<head>
    <title>Calvalus Logout</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<table style="width: 820px; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table class="headerPanel">
                <tr>
                    <td>
                        <img src="images/coastcolour-logo.png" alt="Coastcolour logo">
                    </td>
                    <td>
                        <h1 class="title">Calvalus</h1>

                        <h2 class="subTitle">Portal for CoastColour on-demand processing</h2>
                    </td>
                    <td class="userName">
                        <a href="about.jsp">About</a>
                    </td>
                </tr>
            </table>
            <hr/>

            <%@ page session="true" %>

            <div class="loginPanel">

                User <strong><%=request.getRemoteUser()%></strong> has been logged out.

                <% session.invalidate(); %>

                <br/><br/>
                <a href='<%= response.encodeURL("calvalus.jsp") %>'>Click here to login again.</a>
                <br/><br/>

            </div>

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>


</body>
</html>
