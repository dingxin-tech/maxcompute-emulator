package tech.dingxin.maxcompute.aspect;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class AccessAspect {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccessAspect.class);

    @Pointcut("execution(* tech.dingxin.maxcompute.controller.*.*(..))")
    public void controllerMethods() {
    }

    @Before("controllerMethods()")
    public void logControllerAccess(JoinPoint joinPoint) {
        String methodName = joinPoint.getSignature().getName();
        LOGGER.info("Accessed method: {}", methodName);
    }
}