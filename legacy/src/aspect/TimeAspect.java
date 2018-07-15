
/**   
 * @Title: Timer.java 
 * @Package: au.edu.rmit.pagerank.aspect 
 * @Description: TODO
 * @author forrest0402  
 * @date Oct 20, 2017 4:21:21 PM 
 */

package au.edu.rmit.trajectory.similarity.aspect;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * @Description
 * @author forrest0402
 * @date Oct 20, 2017 4:21:21 PM
 */
@Component
@Aspect
public class TimeAspect {

    private static Logger logger = Logger.getLogger(TimeAspect.class);

    //@Around("execution(public * au.edu.rmit.trajectory.similarity.algorithm.TrajectoryMapping.*(..))")
    //@Around("execution(public * au.edu.rmit.trajectory.similarity.service.*.*(..))")
    public Object printExecTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long begin = System.nanoTime();
        Object o = joinPoint.proceed();
        long end = System.nanoTime();
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(joinPoint.getTarget().getClass()).append(".").append(joinPoint.getSignature().getName())
                .append(" : ").append((end - begin) / 1000000).append(" ms");
        logger.info(strBuilder.toString());
        return o;
    }



}
