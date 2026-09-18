<#-- Backend-minimal account set-up email (admin invite flow, execute-actions
     email — camelCase filename, a KC26 gotcha). ``user.username`` is
     user-controlled input rendered into HTML -> kcSanitize it. -->
<html>
<body>
<p>Welcome to scalable ecommerce backend.</p>
<p>Dear ${kcSanitize(user.username)},</p>
<p>to activate your account please click on this link:</p>
<p><a href="${link}">Activate your account</a></p>
<p>This link will expire within ${linkExpirationFormatter(linkExpiration)}.</p>
<p>If you didn't create this account, just ignore this message.</p>
</body>
</html>
