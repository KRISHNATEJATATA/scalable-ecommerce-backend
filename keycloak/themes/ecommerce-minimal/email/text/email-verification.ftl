<#-- Backend-minimal verification email (signup flow: realm ``verifyEmail=true``
     sends this via sendVerifyEmail -> text|html/email-verification.ftl).
     ``user`` (ProfileBean), ``link``, ``linkExpiration`` and the
     ``linkExpirationFormatter`` method bean are in the email FT data model. -->
Welcome to scalable ecommerce backend.

Dear ${user.username},

to activate your account please click on this link:

${link}

This link will expire within ${linkExpirationFormatter(linkExpiration)}.

If you didn't create this account, just ignore this message.
