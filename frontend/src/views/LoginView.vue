<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { User, Lock } from '@element-plus/icons-vue'
import api from '../api/client'

const router = useRouter()
const loading = ref(false)
const form = reactive({
  username: '',
  password: '',
})

async function submit() {
  if (!form.username || !form.password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  loading.value = true
  try {
    const resp = await api.post('/api/auth/login', {
      username: form.username,
      password: form.password,
    })
    localStorage.setItem('token', resp.data.token)
    localStorage.setItem('user', JSON.stringify(resp.data.user))
    ElMessage.success('欢迎回来')
    router.push('/')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '登录失败，请检查账号密码')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-container">
    <div class="login-box">
      <div class="login-header">
        <img src="../assets/vue.svg" alt="Logo" class="logo" />
        <h2>教学资源管理系统</h2>
        <p>基于知识图谱的智能化教学平台</p>
      </div>
      
      <el-card class="login-card" shadow="always">
        <el-form :model="form" size="large" @submit.prevent="submit">
          <el-form-item>
            <el-input 
              v-model="form.username" 
              placeholder="用户名" 
              :prefix-icon="User"
              autocomplete="username" 
            />
          </el-form-item>
          <el-form-item>
            <el-input 
              v-model="form.password" 
              type="password" 
              placeholder="密码" 
              :prefix-icon="Lock"
              autocomplete="current-password" 
              show-password 
            />
          </el-form-item>
          <div class="form-options">
            <el-checkbox label="记住我" />
            <el-link type="primary" :underline="false">忘记密码？</el-link>
          </div>
          <el-button 
            type="primary" 
            class="login-button" 
            :loading="loading" 
            @click="submit"
          >
            立即登录
          </el-button>
        </el-form>
      </el-card>
      
      <div class="login-footer">
        <span>还没有账号？</span>
        <el-link type="primary" :underline="false">联系管理员创建</el-link>
      </div>
    </div>
  </div>
</template>

<style scoped>
.login-container {
  height: 100vh;
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1000;
}

.login-box {
  width: 400px;
  animation: fadeIn 0.8s ease-out;
}

.login-header {
  text-align: center;
  margin-bottom: 30px;
}

.logo {
  width: 60px;
  height: 60px;
  margin-bottom: 16px;
}

.login-header h2 {
  margin: 0;
  font-size: 24px;
  color: #303133;
  font-weight: 600;
}

.login-header p {
  margin: 8px 0 0;
  font-size: 14px;
  color: #909399;
}

.login-card {
  border: none;
  border-radius: 12px;
  padding: 10px;
}

.form-options {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.login-button {
  width: 100%;
  height: 45px;
  font-size: 16px;
  border-radius: 8px;
  letter-spacing: 2px;
}

.login-footer {
  text-align: center;
  margin-top: 24px;
  font-size: 14px;
  color: #606266;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 适配移动端 */
@media (max-width: 480px) {
  .login-box {
    width: 90%;
  }
}
</style>
