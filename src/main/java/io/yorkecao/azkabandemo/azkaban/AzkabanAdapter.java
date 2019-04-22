package io.yorkecao.azkabandemo.azkaban;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.yorkecao.azkabandemo.config.AzkabanDemoConfiguration;
import io.yorkecao.azkabandemo.exception.AzkabanException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Yorke
 */
@Slf4j
@Component
public class AzkabanAdapter {

    @Autowired
    private AzkabanDemoConfiguration config;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private RestTemplate restTemplate;

    private static String SESSION_ID;

    /**
     * 登录
     */
    public void login() {
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("action", "login");
        params.add("username", config.getUsername());
        params.add("password", config.getPassword());

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl(), httpEntity, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                SESSION_ID = respRoot.get("session.id").asText();
                log.info("Azkaban login success:{}", respRoot);
            } else {
                log.warn("Azkaban login failure:{}", respRoot);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban login failure: %s !", e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 创建项目
     *
     * @param projectName 项目名称
     * @param description 项目描述
     */
    public void createProject(String projectName, String description) {
        LinkedMultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("session.id", SESSION_ID);
        parameters.add("action", "create");
        parameters.add("name", projectName);
        parameters.add("description", description);

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(parameters, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/manager", httpEntity, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azcaban create a Project: {}", projectName);
            } else {
                String errorMessage = respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.";
                log.error("Azcaban create Project %s failure: %s", projectName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azcaban create Project %s failure: %s", projectName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 删除项目
     *
     * @param projectName 项目名称
     * @return 删除结果
     */
    public void deleteProject(String projectName) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", SESSION_ID);
        params.put("project", projectName);

        try {
            restTemplate.getForObject(config.getUrl() + "/manager?session.id={id}&delete=true&project={project}", String.class, params);
            log.info("Azkaban delete project: {}", projectName);
        } catch (Exception e) {
            log.error(String.format("Azkaban delete Project %s failure!", projectName), e);
        }
    }

    /**
     * 为项目上传 Zip 文件
     *
     * @param projectName 项目名称
     * @param zipFilePath zip路径
     */
    public void uploadZip(String projectName, String zipFilePath) {
        FileSystemResource file = new FileSystemResource(new File(zipFilePath));
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "upload");
        params.add("project", projectName);
        params.add("file", file);

        String respResult = restTemplate.postForObject(config.getUrl() + "/manager", params, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azcaban Upload a Project Zip to {}: {}", projectName, zipFilePath);
            } else {
                log.error(String.format("Azcaban upload Project Zip to %s failure: %s", projectName, respRoot.get("error").asText()));
                throw new AzkabanException(respRoot.get("error").asText());
            }
        } catch (IOException e) {
            log.error(String.format("Azcaban upload Project Zip to %s failure: %s", projectName, e.getMessage()));
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 查询项目 flow
     *
     * @param projectName 项目名称
     * @return 结果
     */
    public JsonNode fetchProjectFlows(String projectName) {
        Map<String, String> params = new HashMap<>();
        params.put("id", SESSION_ID);
        params.put("project", projectName);

        String respResult = restTemplate.getForObject(config.getUrl() + "/manager?session.id={id}&ajax=fetchprojectflows&project={project}", String.class, params);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azkaban fetch flows of Project {}: {}", projectName, respResult);
                return respRoot;
            } else {
                String errorMessage = respRoot.get("error").asText();
                if (errorMessage.endsWith("doesn't exist.")) {
                    return null;
                }
                log.error(String.format("Azkaban fetch flows of Project {} failure: {}", projectName, errorMessage));
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban fetch of Project %s failure: %s", projectName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    public String fetchFlowJobs(String projectName, String flowId) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, String> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/manager?session.id={id}&ajax=fetchflowgraph&project={project}&flow={flow}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban fetch Jobs of a Flow:{}", exchange);
        return exchange.toString();
    }

    public JsonNode fetchFlowExecutions(String projectName, String flowId, int start, int length) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);
        map.put("start", start);
        map.put("length", length);

        String respResult = restTemplate.getForObject(config.getUrl() + "/manager?session.id={id}&ajax=fetchFlowExecutions&project={project}&flow={flow}&start={start}&length={length}", String.class, map);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                return respRoot;
            } else {
                String errorMessage = respRoot.get("error").asText();
                if (errorMessage.endsWith("doesn't exist.")) {
                    return null;
                }
                log.error("Azkaban fetch Executions of Flow {} failure: {}", flowId, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban fetch Executions of Flow %s failure: %s", flowId, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    public String fetchFlowRunningExecutions(String projectName, String flowId) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/executor?session.id={id}&ajax=getRunning&project={project}&flow={flow}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban fetch Running Executions of a Flow:{}", exchange);
        return exchange.toString();
    }

    public void simpleExecuteFlow(String project, String flow) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", project);
        map.put("flow", flow);

        String respResult = restTemplate.getForObject(config.getUrl() + "/executor?session.id={id}&ajax=executeFlow&project={project}&flow={flow}", String.class, map);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("error")) {
                String errorMessage = respRoot.get("error").asText();
                log.error("Azkaban Execute a Flow {} failure: {}", flow, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban Execute a Flow %s failure: %s", flow, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    public String executeFLow(String projectName, String flowId, Map<String, Object> optionalParams) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");

        Map<String, Object> map = new HashMap<>();
        if (optionalParams != null) {
            map.putAll(optionalParams);
        }
        map.put("session.id", SESSION_ID);
        map.put("ajax", "getRunning");
        map.put("project", projectName);
        map.put("flow", flowId);

        String paramStr = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining("&"));

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/executor?" + paramStr, HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class);

        log.info("Azkban execute a Flow:{}", exchange);
        return exchange.toString();
    }

    public void cancelFlowExecution(String execId) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("execid", execId);

        String respResult = restTemplate.getForObject(config.getUrl() + "/executor?session.id={id}&ajax=cancelFlow&execid={execid}", String.class, map);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("error")) {
                String errorMessage = respRoot.get("error").asText();
                log.error("Azkaban cancel a Execution {} failure: {}", execId, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban cancel a Execution %s failure: %s", execId, e.getMessage()), e);
        }
    }

    /**
     * Schedule a period-based Flow
     *
     * @param projectName  The name of the project
     * @param flowName     The name of the flow
     * @param scheduleTime The time to schedule the flow. Example: 12,00,pm,PDT (Unless UTC is specified, Azkaban will take current server’s default timezone instead)
     * @param scheduleDate The date to schedule the flow. Example: 07/22/2014
     * @param period       Specifies the recursion period. Depends on the “is_recurring” flag being set. Example: 5w
     */
    public void schedulePeriodBasedFlow(String projectName, String flowName, String scheduleDate, String scheduleTime, String period) {
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleFlow");
        params.add("projectName", projectName);
        String projectId = Optional.ofNullable(fetchProjectFlows(projectName).get("projectId")).map(JsonNode::asText).orElse("");
        params.add("projectId", projectId);
        params.add("flow", flowName);
        params.add("scheduleTime", scheduleTime);
        params.add("scheduleDate", scheduleDate);
        if (!StringUtils.isEmpty(period)) {
            params.add("is_recurring", "on");
            params.add("period", period);
        }

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());
        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azkaban schedule a period-based FLow: {}", respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.");
                if (respRoot.hasNonNull("error")) {
                    log.warn("Azkaban schedule period-base Flow error: {}", respRoot.get("error").asText());
                }
            } else {
                String errorMessage = respRoot.hasNonNull("message")
                        ? respRoot.get("message").asText()
                        : (
                        respRoot.hasNonNull("error")
                                ? respRoot.get("error").asText()
                                : "No message."
                );
                log.error("Azkaban schedule period-based FLow {} failure: {}", flowName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban schedule period-based Flow %s failure: %s", flowName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 调度一个基于 Cron 的 Flow
     *
     * @param projectName    项目名
     * @param flowName       Flow 名
     * @param cronExpression Cron 表达式
     */
    public void scheduleCronBasedFlow(String projectName, String flowName, String cronExpression) {
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleCronFlow");
        params.add("projectName", projectName);
        params.add("flow", flowName);
        params.add("cronExpression", cronExpression);

        HttpEntity<LinkedMultiValueMap<String, Object>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azkaban schedule a Cron Flow: {}", respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.");
                if (respRoot.hasNonNull("error")) {
                    log.warn("Azkaban schedule Cron Flow with error: {}", respRoot.get("error").asText());
                }
            } else {
                String errorMessage = respRoot.hasNonNull("message")
                        ? respRoot.get("message").asText()
                        : (
                        respRoot.hasNonNull("error")
                                ? respRoot.get("error").asText()
                                : "No message."
                );
                log.error("Azkaban schedule Cron Flow {} failure: {}", flowName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban schedule Cron Flow {} failure: {}", flowName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 获取一个 Schedule
     * @param projectId 项目 ID
     * @param flowId Flow ID
     * @return Schedule
     */
    public JsonNode fetchSchedule(String projectId, String flowId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", SESSION_ID);
        params.put("projectId", projectId);
        params.put("flowId", flowId);

        try {
            String respResult = restTemplate.getForObject(config.getUrl() + "/schedule?session.id={id}&ajax=fetchSchedule&projectId={projectId}&flowId={flowId}", String.class, params);
            return objectMapper.readTree(respResult);
        } catch (IOException e) {
            log.error(String.format("Azkaban fetch Schedule of Flow %s failure: %s", flowId, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * Flexible scheduling using Cron
     *
     * @param projectName    The name of the project
     * @param flowName       The name of the flow
     * @param cronExpression A CRON expression is a string comprising 6 or 7 fields separated by white space that represents a set of times
     * @return Response data
     */
    public String scheduleFlow(String projectName, String flowName, String cronExpression) throws IOException {
        HttpHeaders httpHeaders = getAzkabanHeaders();

        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleCronFlow");
        params.add("projectName", projectName);
        params.add("flow", flowName);
        params.add("cronExpression", cronExpression);

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, httpHeaders);

        String respData = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);

        log.info("Azkaban flexible scheduling using Cron: {}", respData);

        return respData;
    }

    /**
     * 移除一个调度
     * @param scheduleId Schedule ID
     */
    public void unscheduleFlow(String scheduleId) {
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("action", "removeSched");
        params.add("scheduleId", scheduleId);

        HttpEntity<LinkedMultiValueMap<String, Object>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azkaban unschedule a Flow: {}", respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.");
            } else {
                String errorMessage = respRoot.hasNonNull("message")
                        ? respRoot.get("message").asText()
                        : (
                        respRoot.hasNonNull("error")
                                ? respRoot.get("error").asText()
                                : "No message."
                );
                log.error("Azkaban unschedule Flow {} failure: {}", scheduleId, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban unschedule Flow %s failure: %s", scheduleId, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    public void setSla(String scheduleId, String[] slaEmails, String[][] settings) {
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "setSla");
        params.add("scheduleId", scheduleId);
        params.add("slaEmails", Optional.ofNullable(slaEmails).map(sm -> String.join(";", sm)).orElse(""));
        for (int i = 0; i < settings.length; i++) {
            params.add(String.format("settings[%d]", i), settings[i]);
        }

        HttpEntity<LinkedMultiValueMap<String, Object>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("error")) {
                log.error("Azkaban set SLA for Scheduled {} failure: {}", scheduleId, respRoot.get("error").asText());
                throw new AzkabanException(respRoot.get("error").asText());
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban set SLA for Scheduled %s failure: %s", scheduleId, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 暂停一个 Execution
     * @param execid Execution ID
     */
    public void pauseFlowExecution(String execid) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("execid", execid);

        String respResult = restTemplate.getForObject(config.getUrl() + "/executor?session.id={id}&ajax=pauseFlow&execid={execid}", String.class, map);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azkaban paused a Flow: %s", execid);
            } else {
                log.error("Azkaban pause Flow Execution {} failure: {}", execid, respRoot.get("error").asText());
                throw new AzkabanException(respRoot.get("error").asText());
            }
        } catch (IOException e) {
            log.error("Azkaban pause Flow Execution {} failure: {}", execid, e.getMessage());
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 恢复一个 Execution
     * @param execid Execution ID
     */
    public void resumeFlowExecution(String execid) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("execid", execid);

        String respResult = restTemplate.getForObject(config.getUrl() + "/executor?session.id={id}&ajax=resumeFlow&execid={execid}", String.class, map);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azkaban resumed a Flow: {}", execid);
            } else {
                log.error("Azkaban resume Flow Execution {} failure: {}", execid, respRoot.get("error").asText());
                throw new AzkabanException(respRoot.get("error").asText());
            }
        } catch (IOException e) {
            log.error("Azkaban resume Flow Execution {} failure: {}", execid, e.getMessage());
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 执行 flow
     *
     * @param projectName 项目名称
     * @param flowName    flow 名称
     * @return 执行 ID
     */
    public String startFlow(String projectName, String flowName) throws IOException {
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<String, Object>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "executeFlow");
        linkedMultiValueMap.add("project", projectName);
        linkedMultiValueMap.add("flow", flowName);
        String res = restTemplate.postForObject(config.getUrl() + "/executor", linkedMultiValueMap, String.class);
        log.info("azkaban start flow:{}", res);
        JsonNode objectNode = objectMapper.readTree(res);
        return objectNode.get("execid").asText();
    }

    /**
     * 执行信息
     *
     * @param execId 执行ID
     * @return 结果
     */
    public String executionInfo(String execId) {
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "fetchexecflow");
        linkedMultiValueMap.add("execid", execId);
        String res = restTemplate.postForObject(config.getUrl() + "/executor", linkedMultiValueMap, String.class);
        log.info("azkaban execution info:{}", res);
        return res;
    }



    /**
     * 查询 flow 执行情况
     *
     * @param execId 执行ID
     * @return 结果
     */
    public String fetchFlowExecution(String execId) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=fetchexecflow&session.id={1}&execid={2}"
                        , String.class, SESSION_ID, execId
                );
        log.info("azkban execution flow:{}", res);

        return res;
    }

    /**
     * 执行job日志
     *
     * @param execId 执行ID
     * @param jobId  job ID
     * @param offset 起始位置
     * @param length 长度
     * @return 结果
     */
    public String fetchExecutionJobLogs(String execId, String jobId, int offset, int length) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=fetchExecJobLogs&session.id={1}&execid={2}&jobId={3}&offset={4}&length={5}"
                        , String.class, SESSION_ID, execId, jobId, offset, length
                );
        log.info("azkban execution job logs:{}", res);
        return res;
    }

    private HttpHeaders getAzkabanHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded; charset=utf-8");
        httpHeaders.add("X-Requested-With", "XMLHttpRequest");
        return httpHeaders;
    }
}
